#!/bin/bash
# GCloud Benchmark: Moon vs Redis vs Qdrant
# Instance: e2-highmem-4 (4 vCPU, 32GB RAM, AMD EPYC 7B12)
#
# Scenarios:
#   1. No persistence: Moon vs Redis (KV operations)
#   2. AOF/WAL persistence: Moon vs Redis (KV operations)
#   3. Vector search: Moon vs Redis vs Qdrant
#
# Usage: ./gcloud-benchmark.sh [scenario1|scenario2|scenario3|all]

set -euo pipefail

MOON_BIN="${MOON_BIN:-$HOME/moon/target/release/moon}"
MOON_PORT=6399
REDIS_PORT=6379
QDRANT_PORT=6333
RESULTS_DIR="$HOME/benchmark-results-$(date +%Y%m%d-%H%M%S)"
CLIENTS=50
PIPELINE=16
REQUESTS=1000000
DATASIZE=64

mkdir -p "$RESULTS_DIR"

# Utility functions
kill_servers() {
    pkill -f "moon --port" 2>/dev/null || true
    pkill -f "redis-server" 2>/dev/null || true
    pkill -f "qdrant" 2>/dev/null || true
    sleep 1
}

wait_for_port() {
    local port=$1 max=30
    for i in $(seq 1 $max); do
        if redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; then return 0; fi
        sleep 0.5
    done
    echo "ERROR: Port $port not ready after ${max}s"
    return 1
}

wait_for_http() {
    local port=$1 max=30
    for i in $(seq 1 $max); do
        if curl -s "http://localhost:$port/healthz" >/dev/null 2>&1 || \
           curl -s "http://localhost:$port/" >/dev/null 2>&1; then return 0; fi
        sleep 0.5
    done
    echo "ERROR: HTTP port $port not ready after ${max}s"
    return 1
}

run_redis_benchmark() {
    local label=$1 port=$2 extra_args="${3:-}"
    local outfile="$RESULTS_DIR/${label}.txt"
    echo "--- $label (port $port) ---"

    for cmd in SET GET MSET; do
        echo "  $cmd..."
        if [ "$cmd" = "MSET" ]; then
            redis-benchmark -p "$port" -c "$CLIENTS" -n "$REQUESTS" \
                -P "$PIPELINE" -t mset -d "$DATASIZE" --csv $extra_args \
                >> "$outfile" 2>&1
        else
            redis-benchmark -p "$port" -c "$CLIENTS" -n "$REQUESTS" \
                -P "$PIPELINE" -t "$(echo $cmd | tr '[:upper:]' '[:lower:]')" \
                -d "$DATASIZE" --csv $extra_args \
                >> "$outfile" 2>&1
        fi
    done

    # Pipeline sweep
    echo "  Pipeline sweep (p=1,4,8,16,32,64)..."
    for p in 1 4 8 16 32 64; do
        redis-benchmark -p "$port" -c "$CLIENTS" -n 500000 \
            -P "$p" -t set,get -d "$DATASIZE" --csv $extra_args \
            >> "$RESULTS_DIR/${label}-pipeline-p${p}.txt" 2>&1
    done

    echo "  Done: $outfile"
}

# ===== SCENARIO 1: No Persistence =====
scenario1() {
    echo ""
    echo "=========================================="
    echo "  SCENARIO 1: No Persistence (KV)"
    echo "=========================================="
    kill_servers
    rm -rf /tmp/moon-data /tmp/redis-data

    # Redis - no persistence
    echo "[1/2] Starting Redis (no persist)..."
    redis-server --port $REDIS_PORT --save "" --appendonly no \
        --protected-mode no --daemonize yes --loglevel warning \
        --dir /tmp/redis-data 2>/dev/null
    wait_for_port $REDIS_PORT

    run_redis_benchmark "s1-redis-no-persist" $REDIS_PORT

    # Capture Redis memory
    redis-cli -p $REDIS_PORT INFO memory | grep "used_memory_human" >> "$RESULTS_DIR/s1-redis-no-persist-memory.txt"
    redis-cli -p $REDIS_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1

    # Moon - no persistence (shards=1 for fair comparison, then shards=4)
    for shards in 1 4; do
        echo "[2/2] Starting Moon (no persist, shards=$shards)..."
        "$MOON_BIN" --port $MOON_PORT --shards $shards &
        MOON_PID=$!
        wait_for_port $MOON_PORT

        run_redis_benchmark "s1-moon-no-persist-s${shards}" $MOON_PORT

        # Capture Moon memory
        redis-cli -p $MOON_PORT INFO memory | grep "used_memory_human" >> "$RESULTS_DIR/s1-moon-no-persist-s${shards}-memory.txt" 2>/dev/null || true
        kill $MOON_PID 2>/dev/null || true
        sleep 1
    done

    echo "Scenario 1 complete."
}

# ===== SCENARIO 2: AOF/WAL Persistence =====
scenario2() {
    echo ""
    echo "=========================================="
    echo "  SCENARIO 2: AOF/WAL Persistence (KV)"
    echo "=========================================="
    kill_servers
    rm -rf /tmp/moon-data /tmp/redis-data
    mkdir -p /tmp/redis-data /tmp/moon-data

    # Redis - AOF everysec
    echo "[1/2] Starting Redis (AOF everysec)..."
    redis-server --port $REDIS_PORT --save "" --appendonly yes \
        --appendfsync everysec --protected-mode no --daemonize yes \
        --loglevel warning --dir /tmp/redis-data 2>/dev/null
    wait_for_port $REDIS_PORT

    run_redis_benchmark "s2-redis-aof-everysec" $REDIS_PORT

    redis-cli -p $REDIS_PORT INFO memory | grep "used_memory_human" >> "$RESULTS_DIR/s2-redis-aof-memory.txt"
    redis-cli -p $REDIS_PORT INFO persistence | grep "aof_" >> "$RESULTS_DIR/s2-redis-aof-stats.txt"
    redis-cli -p $REDIS_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1

    # Redis - AOF always (strongest durability)
    echo "[extra] Starting Redis (AOF always)..."
    rm -rf /tmp/redis-data/*
    redis-server --port $REDIS_PORT --save "" --appendonly yes \
        --appendfsync always --protected-mode no --daemonize yes \
        --loglevel warning --dir /tmp/redis-data 2>/dev/null
    wait_for_port $REDIS_PORT

    run_redis_benchmark "s2-redis-aof-always" $REDIS_PORT

    redis-cli -p $REDIS_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1

    # Moon - WAL (shards=1, then shards=4)
    for shards in 1 4; do
        echo "[2/2] Starting Moon (WAL, shards=$shards)..."
        rm -rf /tmp/moon-data/*
        "$MOON_BIN" --port $MOON_PORT --shards $shards --aof-enabled \
            --appendfsync everysec --data-dir /tmp/moon-data &
        MOON_PID=$!
        wait_for_port $MOON_PORT

        run_redis_benchmark "s2-moon-wal-everysec-s${shards}" $MOON_PORT

        redis-cli -p $MOON_PORT INFO memory | grep "used_memory_human" >> "$RESULTS_DIR/s2-moon-wal-s${shards}-memory.txt" 2>/dev/null || true
        kill $MOON_PID 2>/dev/null || true
        sleep 1
    done

    # Moon - WAL always
    for shards in 1 4; do
        echo "[extra] Starting Moon (WAL always, shards=$shards)..."
        rm -rf /tmp/moon-data/*
        "$MOON_BIN" --port $MOON_PORT --shards $shards --aof-enabled \
            --appendfsync always --data-dir /tmp/moon-data &
        MOON_PID=$!
        wait_for_port $MOON_PORT

        run_redis_benchmark "s2-moon-wal-always-s${shards}" $MOON_PORT

        kill $MOON_PID 2>/dev/null || true
        sleep 1
    done

    echo "Scenario 2 complete."
}

# ===== SCENARIO 3: Vector Search =====
scenario3() {
    echo ""
    echo "=========================================="
    echo "  SCENARIO 3: Vector Search"
    echo "=========================================="
    kill_servers
    rm -rf /tmp/moon-data /tmp/redis-data /tmp/qdrant-data
    mkdir -p /tmp/redis-data /tmp/moon-data /tmp/qdrant-data

    local DIM=384
    local NUM_VECTORS=50000
    local SEARCH_COUNT=1000

    # --- Generate test data ---
    echo "Generating $NUM_VECTORS vectors (dim=$DIM)..."
    python3 - <<'PYEOF'
import random, struct, os, time, json

DIM = 384
NUM = 50000
SEARCH = 1000

random.seed(42)
vectors = [[random.gauss(0, 1) for _ in range(DIM)] for _ in range(NUM)]

# Save as Redis FT commands
with open("/tmp/vector-insert-redis.txt", "w") as f:
    for i, v in enumerate(vectors):
        blob = struct.pack(f'{DIM}f', *v)
        hex_blob = blob.hex()
        f.write(f"HSET doc:{i} content 'text{i}' embedding {hex_blob}\n")

# Save search queries
with open("/tmp/vector-search-queries.txt", "w") as f:
    for i in range(SEARCH):
        q = vectors[random.randint(0, NUM-1)]  # use existing vector as query
        blob = struct.pack(f'{DIM}f', *q)
        hex_blob = blob.hex()
        f.write(f"{hex_blob}\n")

# Save Qdrant JSON payloads
os.makedirs("/tmp/qdrant-data-import", exist_ok=True)
batch_size = 1000
for batch_start in range(0, NUM, batch_size):
    batch_end = min(batch_start + batch_size, NUM)
    points = []
    for i in range(batch_start, batch_end):
        points.append({
            "id": i,
            "vector": vectors[i],
            "payload": {"content": f"text{i}"}
        })
    with open(f"/tmp/qdrant-data-import/batch_{batch_start}.json", "w") as f:
        json.dump({"points": points}, f)

print(f"Generated {NUM} vectors, {SEARCH} queries")
PYEOF

    # --- Moon Vector Search ---
    echo "[1/3] Moon vector search..."
    "$MOON_BIN" --port $MOON_PORT --shards 1 &
    MOON_PID=$!
    wait_for_port $MOON_PORT

    # Create index
    redis-cli -p $MOON_PORT FT.CREATE idx ON HASH PREFIX 1 doc: \
        SCHEMA content TEXT embedding VECTOR HNSW 6 TYPE FLOAT32 DIM $DIM DISTANCE_METRIC COSINE 2>/dev/null

    # Insert vectors
    MOON_INSERT_START=$(date +%s%N)
    while IFS= read -r line; do
        redis-cli -p $MOON_PORT $line >/dev/null 2>&1
    done < /tmp/vector-insert-redis.txt
    MOON_INSERT_END=$(date +%s%N)
    MOON_INSERT_MS=$(( (MOON_INSERT_END - MOON_INSERT_START) / 1000000 ))
    echo "  Moon insert: ${MOON_INSERT_MS}ms for $NUM_VECTORS vectors"
    echo "moon_insert_ms=$MOON_INSERT_MS" >> "$RESULTS_DIR/s3-vector-results.txt"

    # Search
    MOON_SEARCH_START=$(date +%s%N)
    MOON_SEARCH_OK=0
    while IFS= read -r hex_blob; do
        result=$(redis-cli -p $MOON_PORT FT.SEARCH idx "*=>[KNN 10 @embedding \$vec AS score]" PARAMS 2 vec "$(echo "$hex_blob" | xxd -r -p)" LIMIT 0 10 2>&1)
        if echo "$result" | grep -q "doc:"; then
            MOON_SEARCH_OK=$((MOON_SEARCH_OK + 1))
        fi
    done < /tmp/vector-search-queries.txt
    MOON_SEARCH_END=$(date +%s%N)
    MOON_SEARCH_MS=$(( (MOON_SEARCH_END - MOON_SEARCH_START) / 1000000 ))
    echo "  Moon search: ${MOON_SEARCH_MS}ms for $SEARCH_COUNT queries ($MOON_SEARCH_OK hits)"
    echo "moon_search_ms=$MOON_SEARCH_MS" >> "$RESULTS_DIR/s3-vector-results.txt"
    echo "moon_search_hits=$MOON_SEARCH_OK" >> "$RESULTS_DIR/s3-vector-results.txt"

    redis-cli -p $MOON_PORT INFO memory | grep "used_memory_human" >> "$RESULTS_DIR/s3-moon-memory.txt" 2>/dev/null || true
    kill $MOON_PID 2>/dev/null || true
    sleep 1

    # --- Redis with RediSearch ---
    echo "[2/3] Redis vector search..."
    # Check if Redis has the search module
    redis-server --port $REDIS_PORT --save "" --appendonly no \
        --protected-mode no --daemonize yes --loglevel warning \
        --dir /tmp/redis-data 2>/dev/null
    wait_for_port $REDIS_PORT

    # Try creating index - will fail if no search module
    if redis-cli -p $REDIS_PORT FT.CREATE idx ON HASH PREFIX 1 doc: \
        SCHEMA content TEXT embedding VECTOR HNSW 6 TYPE FLOAT32 DIM $DIM DISTANCE_METRIC COSINE 2>&1 | grep -qi "unknown\|err"; then
        echo "  Redis: FT module not available, skipping vector benchmark"
        echo "redis_vector=NOT_AVAILABLE" >> "$RESULTS_DIR/s3-vector-results.txt"
        redis-cli -p $REDIS_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    else
        # Insert vectors
        REDIS_INSERT_START=$(date +%s%N)
        while IFS= read -r line; do
            redis-cli -p $REDIS_PORT $line >/dev/null 2>&1
        done < /tmp/vector-insert-redis.txt
        REDIS_INSERT_END=$(date +%s%N)
        REDIS_INSERT_MS=$(( (REDIS_INSERT_END - REDIS_INSERT_START) / 1000000 ))
        echo "  Redis insert: ${REDIS_INSERT_MS}ms"
        echo "redis_insert_ms=$REDIS_INSERT_MS" >> "$RESULTS_DIR/s3-vector-results.txt"

        redis-cli -p $REDIS_PORT INFO memory | grep "used_memory_human" >> "$RESULTS_DIR/s3-redis-memory.txt"
        redis-cli -p $REDIS_PORT SHUTDOWN NOSAVE 2>/dev/null || true
    fi
    sleep 1

    # --- Qdrant ---
    echo "[3/3] Qdrant vector search..."
    qdrant --storage-path /tmp/qdrant-data &
    QDRANT_PID=$!
    wait_for_http $QDRANT_PORT

    # Create collection
    curl -s -X PUT "http://localhost:$QDRANT_PORT/collections/test" \
        -H "Content-Type: application/json" \
        -d "{\"vectors\":{\"size\":$DIM,\"distance\":\"Cosine\"}}" >/dev/null

    # Insert vectors
    QDRANT_INSERT_START=$(date +%s%N)
    for batch_file in /tmp/qdrant-data-import/batch_*.json; do
        curl -s -X PUT "http://localhost:$QDRANT_PORT/collections/test/points" \
            -H "Content-Type: application/json" \
            -d @"$batch_file" >/dev/null
    done
    QDRANT_INSERT_END=$(date +%s%N)
    QDRANT_INSERT_MS=$(( (QDRANT_INSERT_END - QDRANT_INSERT_START) / 1000000 ))
    echo "  Qdrant insert: ${QDRANT_INSERT_MS}ms"
    echo "qdrant_insert_ms=$QDRANT_INSERT_MS" >> "$RESULTS_DIR/s3-vector-results.txt"

    # Search
    QDRANT_SEARCH_START=$(date +%s%N)
    QDRANT_SEARCH_OK=0
    python3 - <<'PYEOF2'
import random, struct, json, urllib.request, time

DIM = 384
random.seed(42)
vectors = [[random.gauss(0, 1) for _ in range(DIM)] for _ in range(50000)]

count = 0
for i in range(1000):
    q = vectors[random.randint(0, 49999)]
    data = json.dumps({"vector": q, "limit": 10}).encode()
    req = urllib.request.Request(
        "http://localhost:6333/collections/test/points/search",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    resp = urllib.request.urlopen(req)
    result = json.loads(resp.read())
    if result.get("result"):
        count += 1

print(f"qdrant_search_hits={count}")
PYEOF2
    QDRANT_SEARCH_END=$(date +%s%N)
    QDRANT_SEARCH_MS=$(( (QDRANT_SEARCH_END - QDRANT_SEARCH_START) / 1000000 ))
    echo "  Qdrant search: ${QDRANT_SEARCH_MS}ms for 1000 queries"
    echo "qdrant_search_ms=$QDRANT_SEARCH_MS" >> "$RESULTS_DIR/s3-vector-results.txt"

    kill $QDRANT_PID 2>/dev/null || true
    sleep 1

    echo "Scenario 3 complete."
}

# ===== GENERATE REPORT =====
generate_report() {
    echo ""
    echo "=========================================="
    echo "  GENERATING BENCHMARK REPORT"
    echo "=========================================="

    cat > "$RESULTS_DIR/REPORT.md" <<HEADER
# Moon Benchmark Report
## Instance: GCP e2-highmem-4 (4 vCPU, 32GB RAM, AMD EPYC 7B12)
## Date: $(date -u +"%Y-%m-%d %H:%M UTC")

HEADER

    echo "### Scenario 1: No Persistence" >> "$RESULTS_DIR/REPORT.md"
    echo '```' >> "$RESULTS_DIR/REPORT.md"
    for f in "$RESULTS_DIR"/s1-*.txt; do
        echo "=== $(basename "$f") ===" >> "$RESULTS_DIR/REPORT.md"
        cat "$f" >> "$RESULTS_DIR/REPORT.md"
        echo "" >> "$RESULTS_DIR/REPORT.md"
    done
    echo '```' >> "$RESULTS_DIR/REPORT.md"

    echo "### Scenario 2: AOF/WAL Persistence" >> "$RESULTS_DIR/REPORT.md"
    echo '```' >> "$RESULTS_DIR/REPORT.md"
    for f in "$RESULTS_DIR"/s2-*.txt; do
        echo "=== $(basename "$f") ===" >> "$RESULTS_DIR/REPORT.md"
        cat "$f" >> "$RESULTS_DIR/REPORT.md"
        echo "" >> "$RESULTS_DIR/REPORT.md"
    done
    echo '```' >> "$RESULTS_DIR/REPORT.md"

    echo "### Scenario 3: Vector Search" >> "$RESULTS_DIR/REPORT.md"
    echo '```' >> "$RESULTS_DIR/REPORT.md"
    for f in "$RESULTS_DIR"/s3-*.txt; do
        echo "=== $(basename "$f") ===" >> "$RESULTS_DIR/REPORT.md"
        cat "$f" >> "$RESULTS_DIR/REPORT.md"
        echo "" >> "$RESULTS_DIR/REPORT.md"
    done
    echo '```' >> "$RESULTS_DIR/REPORT.md"

    echo "Report: $RESULTS_DIR/REPORT.md"
}

# ===== MAIN =====
echo "Moon GCloud Benchmark Suite"
echo "Instance: e2-highmem-4 (4 vCPU, 32GB, AMD EPYC 7B12)"
echo "Results: $RESULTS_DIR"
echo ""

case "${1:-all}" in
    scenario1) scenario1 ;;
    scenario2) scenario2 ;;
    scenario3) scenario3 ;;
    all)
        scenario1
        scenario2
        scenario3
        generate_report
        ;;
    *)
        echo "Usage: $0 [scenario1|scenario2|scenario3|all]"
        exit 1
        ;;
esac

kill_servers
echo ""
echo "All benchmarks complete. Results in: $RESULTS_DIR"
