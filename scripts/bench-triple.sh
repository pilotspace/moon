#!/usr/bin/env bash
set -uo pipefail
cd /Users/tindang/workspaces/tind-repo/moon
OUT=/tmp/bench-triple.txt
: > "$OUT"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# ── Kill everything ──────────────────────────────────────────
pkill -9 -f "moon --port" 2>/dev/null || true
pkill -9 -f "redis-server.*6399" 2>/dev/null || true
pkill -9 -f "qdrant" 2>/dev/null || true
pkill -9 redis-benchmark 2>/dev/null || true
sleep 2

# ── Start Redis with AOF ─────────────────────────────────────
mkdir -p /tmp/redis-aof
redis-server --port 6399 --bind 127.0.0.1 --protected-mode no \
    --appendonly yes --appendfsync everysec \
    --dir /tmp/redis-aof \
    --daemonize yes --loglevel warning
sleep 1
redis-cli -p 6399 PING > /dev/null 2>&1 || { echo "Redis failed to start"; exit 1; }
log "Redis 8.0.2 (appendonly yes, appendfsync everysec) on :6399"

# ── Start Moon ────────────────────────────────────────────────
./target/release/moon --port 6400 --shards 1 &
MOON_PID=$!
sleep 2
redis-cli -p 6400 PING > /dev/null 2>&1 || { echo "Moon failed to start"; exit 1; }
log "Moon v0.1.0 (1 shard, monoio, per-shard WAL) on :6400"

# ── Start Qdrant ──────────────────────────────────────────────
mkdir -p /tmp/qdrant-storage
/tmp/qdrant --storage-path /tmp/qdrant-storage --grpc-port 6334 --http-port 6333 &>/tmp/qdrant.log &
QDRANT_PID=$!
sleep 3
if curl -s http://localhost:6333/healthz > /dev/null 2>&1; then
    log "Qdrant 1.13.2 on :6333 (REST) / :6334 (gRPC)"
    QDRANT_OK=true
else
    log "Qdrant failed to start (will skip vector benchmarks)"
    QDRANT_OK=false
fi

# ── Helpers ───────────────────────────────────────────────────
bench_redis() {
    local port="$1" cmd="$2" pipeline="$3" clients="$4" n="$5"
    redis-benchmark -p "$port" -c "$clients" -n "$n" -t "$cmd" -P "$pipeline" --csv 2>&1 \
        | grep -v '^"test"' | head -1 | cut -d'"' -f4
}

bench_moon() {
    local cmd="$1" pipeline="$2" clients="$3" n="$4"
    timeout 8 redis-benchmark -p 6400 -c "$clients" -n "$n" -t "$cmd" -P "$pipeline" 2>&1 \
        | tr '\r' '\n' | grep "rps=" | grep -v "rps=0.0" | grep -v "nan" \
        | head -1 | grep -oP 'overall: \K[0-9.]+'
}

ratio() {
    local m="$1" r="$2"
    if [ -n "$m" ] && [ -n "$r" ] && [ "$m" != "--" ] && [ "$r" != "--" ]; then
        echo "scale=2; $m / $r" | bc 2>/dev/null || echo "--"
    else
        echo "--"
    fi
}

# ── Write header ──────────────────────────────────────────────
cat >> "$OUT" <<HEADER
# Moon vs Redis 8.0.2 (AOF) vs Qdrant 1.13.2 — Linux aarch64

**Date:** $(date -Iseconds)
**Platform:** $(uname -srm)
**Redis:** 8.0.2, jemalloc, appendonly=yes, appendfsync=everysec
**Moon:** v0.1.0, 1 shard, monoio io_uring, per-shard WAL (everysec)
**Qdrant:** 1.13.2 (vector benchmarks only)

---

HEADER

# ══════════════════════════════════════════════════════════════
# PART 1: KV COMMANDS — Moon vs Redis (AOF)
# ══════════════════════════════════════════════════════════════

for section in "p=1|1|50|100000" "p=16|16|50|200000" "p=64|64|100|1000000"; do
    IFS='|' read -r title pipeline clients n <<< "$section"

    if [ "$pipeline" = "64" ]; then
        cmds="get set"
    else
        cmds="get set incr lpush rpush lpop rpop sadd spop hset zadd"
    fi

    echo "## KV: $title (c=$clients, n=$n)" >> "$OUT"
    echo "" >> "$OUT"
    echo "| Command | Redis(AOF) | Moon | Moon/Redis |" >> "$OUT"
    echo "|---------|----------:|-----:|:----------:|" >> "$OUT"

    for cmd in $cmds; do
        log "Benchmarking $cmd $title ..."
        r=$(bench_redis 6399 "$cmd" "$pipeline" "$clients" "$n")
        m=$(bench_moon "$cmd" "$pipeline" "$clients" "$n")
        rt=$(ratio "${m:---}" "${r:---}")
        CMD_UP=$(echo "$cmd" | tr 'a-z' 'A-Z')
        printf "| %-7s | %s | %s | %sx |\n" "$CMD_UP" "${r:---}" "${m:---}" "$rt" >> "$OUT"
    done
    echo "" >> "$OUT"
done

# ══════════════════════════════════════════════════════════════
# PART 2: VECTOR SEARCH — Moon vs Qdrant
# ══════════════════════════════════════════════════════════════

echo "## Vector Search: Moon vs Qdrant" >> "$OUT"
echo "" >> "$OUT"

if [ "$QDRANT_OK" = true ]; then
    # Create Qdrant collection
    curl -s -X PUT "http://localhost:6333/collections/bench" \
        -H "Content-Type: application/json" \
        -d '{"vectors":{"size":128,"distance":"Cosine"}}' > /dev/null 2>&1

    # Create Moon vector index
    redis-cli -p 6400 FT.CREATE bench_idx ON HASH PREFIX 1 vec: SCHEMA embedding VECTOR FLAT 6 DIM 128 DISTANCE_METRIC COSINE TYPE FLOAT32 > /dev/null 2>&1

    log "Inserting 10K vectors into Qdrant..."
    # Batch insert 10K vectors into Qdrant
    QDRANT_INSERT_START=$(date +%s%N)
    for batch_start in $(seq 0 100 9900); do
        points="["
        for i in $(seq $batch_start $((batch_start + 99))); do
            vec=$(python3 -c "import random; random.seed($i); print([round(random.gauss(0,1),4) for _ in range(128)])")
            [ "$i" -gt "$batch_start" ] && points+=","
            points+="{\"id\":$i,\"vector\":$vec}"
        done
        points+="]"
        curl -s -X PUT "http://localhost:6333/collections/bench/points" \
            -H "Content-Type: application/json" \
            -d "{\"points\":$points}" > /dev/null 2>&1
    done
    QDRANT_INSERT_END=$(date +%s%N)
    QDRANT_INSERT_MS=$(( (QDRANT_INSERT_END - QDRANT_INSERT_START) / 1000000 ))
    log "Qdrant: 10K vectors inserted in ${QDRANT_INSERT_MS}ms"

    log "Inserting 10K vectors into Moon..."
    # Insert 10K vectors into Moon via HSET + blob
    MOON_INSERT_START=$(date +%s%N)
    for i in $(seq 0 9999); do
        vec_hex=$(python3 -c "
import struct, random
random.seed($i)
v = [random.gauss(0,1) for _ in range(128)]
print(struct.pack('128f', *v).hex())
")
        redis-cli -p 6400 HSET "vec:$i" embedding "$vec_hex" > /dev/null 2>&1
    done
    MOON_INSERT_END=$(date +%s%N)
    MOON_INSERT_MS=$(( (MOON_INSERT_END - MOON_INSERT_START) / 1000000 ))
    log "Moon: 10K vectors inserted in ${MOON_INSERT_MS}ms"

    # Query benchmark — 100 queries
    log "Running 100 search queries on Qdrant..."
    QDRANT_QUERY_START=$(date +%s%N)
    for q in $(seq 0 99); do
        qvec=$(python3 -c "import random; random.seed(${q}+50000); print([round(random.gauss(0,1),4) for _ in range(128)])")
        curl -s -X POST "http://localhost:6333/collections/bench/points/search" \
            -H "Content-Type: application/json" \
            -d "{\"vector\":$qvec,\"limit\":10}" > /dev/null 2>&1
    done
    QDRANT_QUERY_END=$(date +%s%N)
    QDRANT_QPS=$(python3 -c "print(f'{100 / (($QDRANT_QUERY_END - $QDRANT_QUERY_START) / 1e9):.1f}')")

    log "Running 100 search queries on Moon..."
    MOON_QUERY_START=$(date +%s%N)
    for q in $(seq 0 99); do
        qvec_blob=$(python3 -c "
import struct, random
random.seed(${q}+50000)
v = [random.gauss(0,1) for _ in range(128)]
print(struct.pack('128f', *v).hex())
")
        redis-cli -p 6400 FT.SEARCH bench_idx "*=>[KNN 10 @embedding \$BLOB AS score]" PARAMS 2 BLOB "$qvec_blob" DIALECT 2 > /dev/null 2>&1
    done
    MOON_QUERY_END=$(date +%s%N)
    MOON_QPS=$(python3 -c "print(f'{100 / (($MOON_QUERY_END - $MOON_QUERY_START) / 1e9):.1f}')")

    cat >> "$OUT" <<VECRESULT
| Metric | Moon | Qdrant |
|--------|-----:|-------:|
| Insert 10K (128d) | ${MOON_INSERT_MS}ms | ${QDRANT_INSERT_MS}ms |
| Search QPS (k=10) | ${MOON_QPS} | ${QDRANT_QPS} |

VECRESULT
else
    echo "Qdrant not available — vector comparison skipped." >> "$OUT"
    echo "" >> "$OUT"
fi

# ── Cleanup ───────────────────────────────────────────────────
kill $MOON_PID 2>/dev/null || true
[ -n "${QDRANT_PID:-}" ] && kill $QDRANT_PID 2>/dev/null || true
redis-cli -p 6399 SHUTDOWN NOSAVE 2>/dev/null || true
rm -rf /tmp/redis-aof /tmp/qdrant-storage 2>/dev/null || true

echo "=== DONE ===" >> "$OUT"
cat "$OUT"
