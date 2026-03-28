#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-production.sh -- Production use-case benchmark suite
#
# Tests real-world Redis patterns: session stores, rate limiting, leaderboards,
# caching layers, pub/sub fanout, time-series ingest, and mixed workloads.
# Compares moon vs Redis side-by-side with latency percentiles.
#
# Usage:
#   ./bench-production.sh                  # Run all scenarios
#   ./bench-production.sh --scenario NAME  # Run single scenario
#   ./bench-production.sh --list           # List available scenarios
#   ./bench-production.sh --shards N       # moon shard count (default: 1)
#   ./bench-production.sh --duration SEC   # Test duration per scenario (default: 30)
#   ./bench-production.sh --output FILE    # Output file (default: BENCHMARK-PRODUCTION.md)
###############################################################################

# ===========================================================================
# Configuration
# ===========================================================================

PORT_REDIS=6399
PORT_RUST=6400
SHARDS=1
DURATION=30
REQUESTS=200000
OUTPUT_FILE="BENCHMARK-PRODUCTION.md"
RUST_BINARY="./target/release/moon"
SCENARIO_FILTER=""
RESULTS_DIR=""

RUST_PID=""
REDIS_PID=""

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
    [[ -n "${RESULTS_DIR:-}" ]] && [[ -d "$RESULTS_DIR" ]] && rm -rf "$RESULTS_DIR"
}
trap cleanup EXIT

parse_rps() {
    # Redis-benchmark 8.x uses \r for progress, final line has "requests per second"
    # Convert \r to \n first, then extract the numeric RPS value
    tr '\r' '\n' | grep "requests per second" | tail -1 | awk '{print $2}' | sed 's/,//g'
}

parse_p50() {
    tr '\r' '\n' | grep "requests per second" | tail -1 | sed 's/.*p50=\([0-9.]*\).*/\1/'
}

run_redis_bench() {
    local port=$1; shift
    redis-benchmark -p "$port" -q "$@" 2>/dev/null
}

wait_for_server() {
    local port=$1 name=$2 retries=20
    while ! redis-cli -p "$port" PING &>/dev/null; do
        retries=$((retries - 1))
        [[ $retries -le 0 ]] && { log "ERROR: $name on port $port not responding"; exit 1; }
        sleep 0.2
    done
}

flush_server() {
    redis-cli -p "$1" FLUSHALL &>/dev/null || true
}

# Get RSS in KB
get_rss_kb() {
    local port=$1
    local pid
    if [[ "$port" == "$PORT_RUST" ]]; then
        pid="$RUST_PID"
    else
        pid=$(lsof -ti :"$port" 2>/dev/null | head -1)
    fi
    [[ -z "$pid" ]] && echo "0" && return
    ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0"
}

format_number() {
    # Format float as integer with commas: 58823.53 → 58,823
    local n="${1%%.*}"  # strip decimal
    [[ -z "$n" ]] && echo "0" && return
    printf "%'d" "$n" 2>/dev/null || echo "$n"
}

ratio() {
    local a="${1%%.*}" b="${2%%.*}"  # strip decimals for ratio
    [[ -z "$a" ]] && a=0
    [[ -z "$b" ]] && b=0
    if [[ "$b" == "0" ]]; then echo "N/A"; return; fi
    awk "BEGIN { printf \"%.2fx\", $a / $b }"
}

# ===========================================================================
# Scenario Definitions
# ===========================================================================

scenario_session_store() {
    # Simulates web session store: SET with TTL, GET for auth checks, DEL on logout
    # Real-world: 80% GET (session validation), 15% SET (login/refresh), 5% DEL (logout)
    local name="Session Store (80% GET / 15% SET / 5% DEL)"
    log "Scenario: $name"

    local results_redis results_rust

    # Pre-populate 50K sessions
    for port in $PORT_REDIS $PORT_RUST; do
        flush_server "$port"
        redis-benchmark -p "$port" -c 10 -n 50000 -t set -r 50000 -d 512 -q &>/dev/null
    done

    # Mixed read-heavy workload
    echo "### $name"
    echo ""

    # GET-heavy (session validation)
    local rps_redis rps_rust p50_redis p50_rust
    local out_redis out_rust

    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t get -r 50000 -d 512)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t get -r 50000 -d 512)
    local get_redis=$(echo "$out_redis" | parse_rps)
    local get_rust=$(echo "$out_rust" | parse_rps)
    local get_p50_redis=$(echo "$out_redis" | parse_p50)
    local get_p50_rust=$(echo "$out_rust" | parse_p50)

    # SET (login/refresh with 3600s TTL simulation — redis-benchmark can't set TTL, use SET)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $((REQUESTS / 5)) -t set -r 50000 -d 512)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $((REQUESTS / 5)) -t set -r 50000 -d 512)
    local set_redis=$(echo "$out_redis" | parse_rps)
    local set_rust=$(echo "$out_rust" | parse_rps)

    # Pipeline=8 (batch session checks)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t get -r 50000 -d 512 -P 8)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t get -r 50000 -d 512 -P 8)
    local get8_redis=$(echo "$out_redis" | parse_rps)
    local get8_rust=$(echo "$out_rust" | parse_rps)

    echo "| Operation | Redis | moon | Ratio |"
    echo "|-----------|------:|----------:|------:|"
    printf "| GET (session check, p=1) | %s | %s | %s |\n" \
        "$(format_number "${get_redis%%.*}")" "$(format_number "${get_rust%%.*}")" "$(ratio "${get_rust%%.*}" "${get_redis%%.*}")"
    printf "| SET (login, 512B, p=1) | %s | %s | %s |\n" \
        "$(format_number "${set_redis%%.*}")" "$(format_number "${set_rust%%.*}")" "$(ratio "${set_rust%%.*}" "${set_redis%%.*}")"
    printf "| GET (batch check, p=8) | %s | %s | %s |\n" \
        "$(format_number "${get8_redis%%.*}")" "$(format_number "${get8_rust%%.*}")" "$(ratio "${get8_rust%%.*}" "${get8_redis%%.*}")"
    printf "| GET p50 latency | %sms | %sms | |\n" "$get_p50_redis" "$get_p50_rust"
    echo ""
}

scenario_rate_limiter() {
    # Sliding window rate limiter: INCR + EXPIRE pattern
    # Real-world: high-frequency INCR with TTL, occasional GET to check
    local name="Rate Limiter (INCR + EXPIRE pattern)"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do flush_server "$port"; done

    echo "### $name"
    echo ""

    local out_redis out_rust

    # INCR (counter increment — the hot path)
    out_redis=$(run_redis_bench $PORT_REDIS -c 100 -n $REQUESTS -t incr -r 10000)
    out_rust=$(run_redis_bench $PORT_RUST -c 100 -n $REQUESTS -t incr -r 10000)
    local incr_redis=$(echo "$out_redis" | parse_rps)
    local incr_rust=$(echo "$out_rust" | parse_rps)
    local incr_p50_redis=$(echo "$out_redis" | parse_p50)
    local incr_p50_rust=$(echo "$out_rust" | parse_p50)

    # INCR pipelined (batch rate checks)
    out_redis=$(run_redis_bench $PORT_REDIS -c 100 -n $REQUESTS -t incr -r 10000 -P 16)
    out_rust=$(run_redis_bench $PORT_RUST -c 100 -n $REQUESTS -t incr -r 10000 -P 16)
    local incr16_redis=$(echo "$out_redis" | parse_rps)
    local incr16_rust=$(echo "$out_rust" | parse_rps)

    # High concurrency (200 clients — API gateway pattern)
    out_redis=$(run_redis_bench $PORT_REDIS -c 200 -n $REQUESTS -t incr -r 10000)
    out_rust=$(run_redis_bench $PORT_RUST -c 200 -n $REQUESTS -t incr -r 10000)
    local incr200_redis=$(echo "$out_redis" | parse_rps)
    local incr200_rust=$(echo "$out_rust" | parse_rps)

    echo "| Operation | Redis | moon | Ratio |"
    echo "|-----------|------:|----------:|------:|"
    printf "| INCR (p=1, 100 clients) | %s | %s | %s |\n" \
        "$(format_number "${incr_redis%%.*}")" "$(format_number "${incr_rust%%.*}")" "$(ratio "${incr_rust%%.*}" "${incr_redis%%.*}")"
    printf "| INCR (p=16, 100 clients) | %s | %s | %s |\n" \
        "$(format_number "${incr16_redis%%.*}")" "$(format_number "${incr16_rust%%.*}")" "$(ratio "${incr16_rust%%.*}" "${incr16_redis%%.*}")"
    printf "| INCR (p=1, 200 clients) | %s | %s | %s |\n" \
        "$(format_number "${incr200_redis%%.*}")" "$(format_number "${incr200_rust%%.*}")" "$(ratio "${incr200_rust%%.*}" "${incr200_redis%%.*}")"
    printf "| INCR p50 latency | %sms | %sms | |\n" "$incr_p50_redis" "$incr_p50_rust"
    echo ""
}

scenario_leaderboard() {
    # Gaming leaderboard: ZADD scores, ZRANGEBYSCORE for top-N, ZINCRBY updates
    local name="Leaderboard (Sorted Sets)"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do
        flush_server "$port"
        # Pre-populate leaderboard with 10K members
        redis-benchmark -p "$port" -c 10 -n 10000 -t zadd -r 10000 -q &>/dev/null
    done

    echo "### $name"
    echo ""

    local out_redis out_rust

    # ZADD (score updates)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t zadd -r 10000)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t zadd -r 10000)
    local zadd_redis=$(echo "$out_redis" | parse_rps)
    local zadd_rust=$(echo "$out_rust" | parse_rps)

    # ZADD pipelined (batch score ingestion)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t zadd -r 10000 -P 16)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t zadd -r 10000 -P 16)
    local zadd16_redis=$(echo "$out_redis" | parse_rps)
    local zadd16_rust=$(echo "$out_rust" | parse_rps)

    # ZRANGEBYSCORE (top-N queries) — redis-benchmark supports this
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t zrangebyscore)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t zrangebyscore)
    local zrange_redis=$(echo "$out_redis" | parse_rps)
    local zrange_rust=$(echo "$out_rust" | parse_rps)
    local zrange_p50_redis=$(echo "$out_redis" | parse_p50)
    local zrange_p50_rust=$(echo "$out_rust" | parse_p50)

    echo "| Operation | Redis | moon | Ratio |"
    echo "|-----------|------:|----------:|------:|"
    printf "| ZADD (score update, p=1) | %s | %s | %s |\n" \
        "$(format_number "${zadd_redis%%.*}")" "$(format_number "${zadd_rust%%.*}")" "$(ratio "${zadd_rust%%.*}" "${zadd_redis%%.*}")"
    printf "| ZADD (batch ingest, p=16) | %s | %s | %s |\n" \
        "$(format_number "${zadd16_redis%%.*}")" "$(format_number "${zadd16_rust%%.*}")" "$(ratio "${zadd16_rust%%.*}" "${zadd16_redis%%.*}")"
    printf "| ZRANGEBYSCORE (top-N, p=1) | %s | %s | %s |\n" \
        "$(format_number "${zrange_redis%%.*}")" "$(format_number "${zrange_rust%%.*}")" "$(ratio "${zrange_rust%%.*}" "${zrange_redis%%.*}")"
    printf "| ZRANGEBYSCORE p50 latency | %sms | %sms | |\n" "$zrange_p50_redis" "$zrange_p50_rust"
    echo ""
}

scenario_cache_layer() {
    # Application cache: large values (1KB-4KB), high GET:SET ratio, TTL-based expiry
    local name="Cache Layer (1KB-4KB values, 90% GET / 10% SET)"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do
        flush_server "$port"
        # Pre-populate 20K cache entries with 1KB values
        redis-benchmark -p "$port" -c 10 -n 20000 -t set -r 20000 -d 1024 -q &>/dev/null
    done

    echo "### $name"
    echo ""

    local out_redis out_rust

    # GET 1KB values (cache hit path)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t get -r 20000 -d 1024)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t get -r 20000 -d 1024)
    local get1k_redis=$(echo "$out_redis" | parse_rps)
    local get1k_rust=$(echo "$out_rust" | parse_rps)
    local get1k_p50_redis=$(echo "$out_redis" | parse_p50)
    local get1k_p50_rust=$(echo "$out_rust" | parse_p50)

    # SET 4KB values (cache populate)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $((REQUESTS / 4)) -t set -r 20000 -d 4096)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $((REQUESTS / 4)) -t set -r 20000 -d 4096)
    local set4k_redis=$(echo "$out_redis" | parse_rps)
    local set4k_rust=$(echo "$out_rust" | parse_rps)

    # GET 4KB pipelined (batch cache warming)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t get -r 20000 -d 4096 -P 16)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t get -r 20000 -d 4096 -P 16)
    local get4k16_redis=$(echo "$out_redis" | parse_rps)
    local get4k16_rust=$(echo "$out_rust" | parse_rps)

    # MSET 10 keys x 1KB (batch cache update)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $((REQUESTS / 10)) -t mset -d 1024)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $((REQUESTS / 10)) -t mset -d 1024)
    local mset_redis=$(echo "$out_redis" | parse_rps)
    local mset_rust=$(echo "$out_rust" | parse_rps)

    echo "| Operation | Redis | moon | Ratio |"
    echo "|-----------|------:|----------:|------:|"
    printf "| GET 1KB (cache hit, p=1) | %s | %s | %s |\n" \
        "$(format_number "${get1k_redis%%.*}")" "$(format_number "${get1k_rust%%.*}")" "$(ratio "${get1k_rust%%.*}" "${get1k_redis%%.*}")"
    printf "| SET 4KB (cache populate, p=1) | %s | %s | %s |\n" \
        "$(format_number "${set4k_redis%%.*}")" "$(format_number "${set4k_rust%%.*}")" "$(ratio "${set4k_rust%%.*}" "${set4k_redis%%.*}")"
    printf "| GET 4KB (batch warm, p=16) | %s | %s | %s |\n" \
        "$(format_number "${get4k16_redis%%.*}")" "$(format_number "${get4k16_rust%%.*}")" "$(ratio "${get4k16_rust%%.*}" "${get4k16_redis%%.*}")"
    printf "| MSET 10x1KB (batch update) | %s | %s | %s |\n" \
        "$(format_number "${mset_redis%%.*}")" "$(format_number "${mset_rust%%.*}")" "$(ratio "${mset_rust%%.*}" "${mset_redis%%.*}")"
    printf "| GET 1KB p50 latency | %sms | %sms | |\n" "$get1k_p50_redis" "$get1k_p50_rust"
    echo ""
}

scenario_queue() {
    # Job queue: LPUSH producers, RPOP consumers (list as queue)
    local name="Job Queue (LPUSH/RPOP producer-consumer)"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do flush_server "$port"; done

    echo "### $name"
    echo ""

    local out_redis out_rust

    # LPUSH (enqueue)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t lpush -d 256)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t lpush -d 256)
    local lpush_redis=$(echo "$out_redis" | parse_rps)
    local lpush_rust=$(echo "$out_rust" | parse_rps)

    # RPOP (dequeue)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t rpop)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t rpop)
    local rpop_redis=$(echo "$out_redis" | parse_rps)
    local rpop_rust=$(echo "$out_rust" | parse_rps)

    # LPUSH pipelined (batch enqueue)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t lpush -d 256 -P 16)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t lpush -d 256 -P 16)
    local lpush16_redis=$(echo "$out_redis" | parse_rps)
    local lpush16_rust=$(echo "$out_rust" | parse_rps)

    # RPOP pipelined (batch dequeue)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t rpop -P 16)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t rpop -P 16)
    local rpop16_redis=$(echo "$out_redis" | parse_rps)
    local rpop16_rust=$(echo "$out_rust" | parse_rps)

    echo "| Operation | Redis | moon | Ratio |"
    echo "|-----------|------:|----------:|------:|"
    printf "| LPUSH (enqueue 256B, p=1) | %s | %s | %s |\n" \
        "$(format_number "${lpush_redis%%.*}")" "$(format_number "${lpush_rust%%.*}")" "$(ratio "${lpush_rust%%.*}" "${lpush_redis%%.*}")"
    printf "| RPOP (dequeue, p=1) | %s | %s | %s |\n" \
        "$(format_number "${rpop_redis%%.*}")" "$(format_number "${rpop_rust%%.*}")" "$(ratio "${rpop_rust%%.*}" "${rpop_redis%%.*}")"
    printf "| LPUSH (batch enqueue, p=16) | %s | %s | %s |\n" \
        "$(format_number "${lpush16_redis%%.*}")" "$(format_number "${lpush16_rust%%.*}")" "$(ratio "${lpush16_rust%%.*}" "${lpush16_redis%%.*}")"
    printf "| RPOP (batch dequeue, p=16) | %s | %s | %s |\n" \
        "$(format_number "${rpop16_redis%%.*}")" "$(format_number "${rpop16_rust%%.*}")" "$(ratio "${rpop16_rust%%.*}" "${rpop16_redis%%.*}")"
    echo ""
}

scenario_hash_objects() {
    # User profile / object store: HSET/HGET/HMSET multi-field hashes
    local name="Hash Objects (user profiles, config store)"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do flush_server "$port"; done

    echo "### $name"
    echo ""

    local out_redis out_rust

    # HSET (single field update)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t hset -r 10000)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t hset -r 10000)
    local hset_redis=$(echo "$out_redis" | parse_rps)
    local hset_rust=$(echo "$out_rust" | parse_rps)

    # HSET pipelined (batch profile updates)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t hset -r 10000 -P 16)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t hset -r 10000 -P 16)
    local hset16_redis=$(echo "$out_redis" | parse_rps)
    local hset16_rust=$(echo "$out_rust" | parse_rps)

    # SPOP (random sampling — set membership)
    out_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t spop -r 10000)
    out_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t spop -r 10000)
    local spop_redis=$(echo "$out_redis" | parse_rps)
    local spop_rust=$(echo "$out_rust" | parse_rps)

    echo "| Operation | Redis | moon | Ratio |"
    echo "|-----------|------:|----------:|------:|"
    printf "| HSET (field update, p=1) | %s | %s | %s |\n" \
        "$(format_number "${hset_redis%%.*}")" "$(format_number "${hset_rust%%.*}")" "$(ratio "${hset_rust%%.*}" "${hset_redis%%.*}")"
    printf "| HSET (batch update, p=16) | %s | %s | %s |\n" \
        "$(format_number "${hset16_redis%%.*}")" "$(format_number "${hset16_rust%%.*}")" "$(ratio "${hset16_rust%%.*}" "${hset16_redis%%.*}")"
    printf "| SPOP (random sample, p=1) | %s | %s | %s |\n" \
        "$(format_number "${spop_redis%%.*}")" "$(format_number "${spop_rust%%.*}")" "$(ratio "${spop_rust%%.*}" "${spop_redis%%.*}")"
    echo ""
}

scenario_connection_storm() {
    # Microservices: many short-lived connections (1-10 clients) and high concurrency (500 clients)
    local name="Connection Scaling (1 → 500 clients)"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do flush_server "$port"; done

    echo "### $name"
    echo ""

    echo "| Clients | Redis SET/s | moon SET/s | Ratio | Redis p50 | moon p50 |"
    echo "|--------:|----------:|----------------:|------:|----------:|---------------:|"

    for clients in 1 10 50 100 200 500; do
        local out_redis out_rust
        out_redis=$(run_redis_bench $PORT_REDIS -c "$clients" -n $REQUESTS -t set -d 256)
        out_rust=$(run_redis_bench $PORT_RUST -c "$clients" -n $REQUESTS -t set -d 256)
        local rps_redis=$(echo "$out_redis" | parse_rps)
        local rps_rust=$(echo "$out_rust" | parse_rps)
        local p50_redis=$(echo "$out_redis" | parse_p50)
        local p50_rust=$(echo "$out_rust" | parse_p50)

        printf "| %d | %s | %s | %s | %sms | %sms |\n" \
            "$clients" \
            "$(format_number "${rps_redis%%.*}")" \
            "$(format_number "${rps_rust%%.*}")" \
            "$(ratio "${rps_rust%%.*}" "${rps_redis%%.*}")" \
            "$p50_redis" "$p50_rust"
    done
    echo ""
}

scenario_data_sizes() {
    # Varying payload sizes: 8B (counters) → 64KB (JSON documents)
    local name="Data Size Scaling (8B → 64KB)"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do flush_server "$port"; done

    echo "### $name"
    echo ""

    echo "| Value Size | Redis SET/s | moon SET/s | Ratio | Redis GET/s | moon GET/s | Ratio |"
    echo "|-----------:|----------:|----------------:|------:|----------:|----------------:|------:|"

    for size in 8 64 256 1024 4096 16384 65536; do
        local label
        if [[ $size -ge 1024 ]]; then
            label="$((size / 1024))KB"
        else
            label="${size}B"
        fi

        # Pre-populate
        redis-benchmark -p $PORT_REDIS -c 10 -n 5000 -t set -r 5000 -d "$size" -q &>/dev/null
        redis-benchmark -p $PORT_RUST -c 10 -n 5000 -t set -r 5000 -d "$size" -q &>/dev/null

        local out_set_redis out_set_rust out_get_redis out_get_rust
        out_set_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $((REQUESTS / 2)) -t set -r 5000 -d "$size")
        out_set_rust=$(run_redis_bench $PORT_RUST -c 50 -n $((REQUESTS / 2)) -t set -r 5000 -d "$size")
        out_get_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $((REQUESTS / 2)) -t get -r 5000 -d "$size")
        out_get_rust=$(run_redis_bench $PORT_RUST -c 50 -n $((REQUESTS / 2)) -t get -r 5000 -d "$size")

        local set_redis=$(echo "$out_set_redis" | parse_rps)
        local set_rust=$(echo "$out_set_rust" | parse_rps)
        local get_redis=$(echo "$out_get_redis" | parse_rps)
        local get_rust=$(echo "$out_get_rust" | parse_rps)

        printf "| %s | %s | %s | %s | %s | %s | %s |\n" \
            "$label" \
            "$(format_number "${set_redis%%.*}")" \
            "$(format_number "${set_rust%%.*}")" \
            "$(ratio "${set_rust%%.*}" "${set_redis%%.*}")" \
            "$(format_number "${get_redis%%.*}")" \
            "$(format_number "${get_rust%%.*}")" \
            "$(ratio "${get_rust%%.*}" "${get_redis%%.*}")"
    done
    echo ""
}

scenario_memory_efficiency() {
    # Memory usage comparison at various dataset sizes
    local name="Memory Efficiency"
    log "Scenario: $name"

    echo "### $name"
    echo ""

    echo "| Dataset | Redis RSS | moon RSS | Ratio | Per-Key Redis | Per-Key moon |"
    echo "|--------:|----------:|---------------:|------:|--------------:|-------------------:|"

    for count in 10000 50000 100000; do
        for port in $PORT_REDIS $PORT_RUST; do flush_server "$port"; done

        local rss_before_redis=$(get_rss_kb $PORT_REDIS)
        local rss_before_rust=$(get_rss_kb $PORT_RUST)

        # Populate with 256B values
        redis-benchmark -p $PORT_REDIS -c 10 -n "$count" -t set -r "$count" -d 256 -q &>/dev/null
        redis-benchmark -p $PORT_RUST -c 10 -n "$count" -t set -r "$count" -d 256 -q &>/dev/null
        sleep 1

        local rss_after_redis=$(get_rss_kb $PORT_REDIS)
        local rss_after_rust=$(get_rss_kb $PORT_RUST)

        local delta_redis=$((rss_after_redis - rss_before_redis))
        local delta_rust=$((rss_after_rust - rss_before_rust))

        # Per-key bytes
        local pk_redis pk_rust
        if [[ $count -gt 0 ]] && [[ $delta_redis -gt 0 ]]; then
            pk_redis=$(awk "BEGIN { printf \"%.0f\", ($delta_redis * 1024) / $count }")
        else
            pk_redis="N/A"
        fi
        if [[ $count -gt 0 ]] && [[ $delta_rust -gt 0 ]]; then
            pk_rust=$(awk "BEGIN { printf \"%.0f\", ($delta_rust * 1024) / $count }")
        else
            pk_rust="N/A"
        fi

        local label="${count}"
        [[ $count -ge 1000 ]] && label="$(awk "BEGIN { printf \"%.0fK\", $count/1000 }")"

        printf "| %s keys | %s KB | %s KB | %s | %s B | %s B |\n" \
            "$label" \
            "$(format_number "$rss_after_redis")" \
            "$(format_number "$rss_after_rust")" \
            "$(ratio "$rss_after_redis" "$rss_after_rust")" \
            "$pk_redis" "$pk_rust"
    done
    echo ""
}

scenario_pipeline_scaling() {
    # How throughput scales with pipeline depth
    local name="Pipeline Depth Scaling"
    log "Scenario: $name"

    for port in $PORT_REDIS $PORT_RUST; do flush_server "$port"; done

    echo "### $name"
    echo ""

    echo "| Pipeline | Redis SET/s | moon SET/s | Ratio | Redis GET/s | moon GET/s | Ratio |"
    echo "|---------:|----------:|----------------:|------:|----------:|----------------:|------:|"

    for p in 1 2 4 8 16 32 64 128; do
        local out_set_redis out_set_rust out_get_redis out_get_rust
        out_set_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t set -P "$p")
        out_set_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t set -P "$p")
        out_get_redis=$(run_redis_bench $PORT_REDIS -c 50 -n $REQUESTS -t get -P "$p")
        out_get_rust=$(run_redis_bench $PORT_RUST -c 50 -n $REQUESTS -t get -P "$p")

        local set_redis=$(echo "$out_set_redis" | parse_rps)
        local set_rust=$(echo "$out_set_rust" | parse_rps)
        local get_redis=$(echo "$out_get_redis" | parse_rps)
        local get_rust=$(echo "$out_get_rust" | parse_rps)

        printf "| %d | %s | %s | %s | %s | %s | %s |\n" \
            "$p" \
            "$(format_number "${set_redis%%.*}")" \
            "$(format_number "${set_rust%%.*}")" \
            "$(ratio "${set_rust%%.*}" "${set_redis%%.*}")" \
            "$(format_number "${get_redis%%.*}")" \
            "$(format_number "${get_rust%%.*}")" \
            "$(ratio "${get_rust%%.*}" "${get_redis%%.*}")"
    done
    echo ""
}

# ===========================================================================
# Argument Parsing
# ===========================================================================

while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenario) SCENARIO_FILTER="$2"; shift 2 ;;
        --shards) SHARDS="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --output) OUTPUT_FILE="$2"; shift 2 ;;
        --list)
            echo "Available scenarios:"
            echo "  session      - Session store (80% GET / 15% SET / 5% DEL)"
            echo "  ratelimit    - Rate limiter (INCR + EXPIRE pattern)"
            echo "  leaderboard  - Gaming leaderboard (sorted sets)"
            echo "  cache        - Cache layer (1KB-4KB values)"
            echo "  queue        - Job queue (LPUSH/RPOP)"
            echo "  hash         - Hash objects (user profiles)"
            echo "  connections  - Connection scaling (1-500 clients)"
            echo "  datasizes    - Data size scaling (8B-64KB)"
            echo "  memory       - Memory efficiency comparison"
            echo "  pipeline     - Pipeline depth scaling (1-128)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ===========================================================================
# Main
# ===========================================================================

log "Building moon..."
if [[ ! -x "$RUST_BINARY" ]]; then
    cargo build --release 2>&1 | tail -3
fi

# Kill any existing instances
pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
sleep 1

log "Starting Redis on port $PORT_REDIS..."
redis-server --port "$PORT_REDIS" --save "" --appendonly no --daemonize yes --loglevel warning
wait_for_server "$PORT_REDIS" "Redis"

log "Starting moon on port $PORT_RUST ($SHARDS shards)..."
$RUST_BINARY --port "$PORT_RUST" --shards "$SHARDS" &
RUST_PID=$!
wait_for_server "$PORT_RUST" "moon"

REDIS_VERSION=$(redis-cli -p "$PORT_REDIS" INFO server 2>/dev/null | grep redis_version | tr -d '\r' | cut -d: -f2)

# Generate report
{
    echo "# Production Benchmark: moon vs Redis ${REDIS_VERSION}"
    echo ""
    echo "**Date:** $(date '+%Y-%m-%d %H:%M')"
    echo "**Machine:** $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
    echo "**Redis:** ${REDIS_VERSION}"
    echo "**moon:** ${SHARDS} shard(s), Tokio runtime"
    echo "**Tool:** redis-benchmark (co-located)"
    echo "**Requests:** $(format_number $REQUESTS) per test"
    echo ""
    echo "---"
    echo ""

    should_run() {
        [[ -z "$SCENARIO_FILTER" ]] || [[ "$SCENARIO_FILTER" == "$1" ]]
    }

    should_run "session"     && scenario_session_store
    should_run "ratelimit"   && scenario_rate_limiter
    should_run "leaderboard" && scenario_leaderboard
    should_run "cache"       && scenario_cache_layer
    should_run "queue"       && scenario_queue
    should_run "hash"        && scenario_hash_objects
    should_run "connections" && scenario_connection_storm
    should_run "datasizes"   && scenario_data_sizes
    should_run "memory"      && scenario_memory_efficiency
    should_run "pipeline"    && scenario_pipeline_scaling

    echo "---"
    echo ""
    echo "*Generated by bench-production.sh*"
} | tee "$OUTPUT_FILE"

log "Report written to $OUTPUT_FILE"
log "Done."
