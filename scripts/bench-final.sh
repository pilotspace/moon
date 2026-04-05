#!/usr/bin/env bash
set -uo pipefail
cd /Users/tindang/workspaces/tind-repo/moon
OUT=/tmp/bench-final.txt
: > "$OUT"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# ── Kill everything ──────────────────────────────────────────
pkill -9 -f "moon --port" 2>/dev/null || true
pkill -9 -f "redis-server.*6399" 2>/dev/null || true
pkill -9 -f qdrant 2>/dev/null || true
pkill -9 redis-benchmark 2>/dev/null || true
sleep 2

# ── Start Redis with AOF ─────────────────────────────────────
rm -rf /tmp/redis-aof && mkdir -p /tmp/redis-aof
redis-server --port 6399 --bind 127.0.0.1 --protected-mode no \
    --appendonly yes --appendfsync everysec \
    --dir /tmp/redis-aof \
    --daemonize yes --loglevel warning
sleep 1
log "Redis started"

# ── Start Moon ────────────────────────────────────────────────
./target/release/moon --port 6400 --shards 1 &>/dev/null &
MOON_PID=$!
sleep 2
log "Moon started"

# ── Start Qdrant ──────────────────────────────────────────────
rm -rf /tmp/qdrant-data && mkdir -p /tmp/qdrant-data
cat > /tmp/qdrant-config.yaml <<'YCFG'
storage:
  storage_path: /tmp/qdrant-data/storage
  snapshots_path: /tmp/qdrant-data/snapshots
service:
  http_port: 6333
  grpc_port: 6334
YCFG
/tmp/qdrant --config-path /tmp/qdrant-config.yaml &>/tmp/qdrant.log &
QDRANT_PID=$!
sleep 3
QDRANT_OK=false
if curl -sf http://localhost:6333/healthz > /dev/null 2>&1; then
    log "Qdrant started"
    QDRANT_OK=true
else
    log "Qdrant FAILED — $(head -3 /tmp/qdrant.log)"
fi

# ── Extract Moon RPS (handles redis-benchmark 8.x output) ────
moon_rps() {
    local cmd="$1" pipeline="$2" clients="$3" n="$4"
    local raw
    raw=$(timeout 8 redis-benchmark -p 6400 -c "$clients" -n "$n" -t "$cmd" -P "$pipeline" 2>&1)
    echo "$raw" | tr '\r' '\n' | grep "rps=" | grep -v "rps=0.0" | grep -v "nan" \
        | head -1 | sed 's/.*overall: //' | sed 's/).*//'
}

redis_rps() {
    local cmd="$1" pipeline="$2" clients="$3" n="$4"
    redis-benchmark -p 6399 -c "$clients" -n "$n" -t "$cmd" -P "$pipeline" --csv 2>&1 \
        | grep -v '^"test"' | head -1 | cut -d'"' -f4
}

calc_ratio() {
    if [ -n "$1" ] && [ -n "$2" ]; then
        echo "scale=2; $1 / $2" | bc 2>/dev/null || echo "--"
    else
        echo "--"
    fi
}

# ── Header ────────────────────────────────────────────────────
cat >> "$OUT" <<HDR
# Moon vs Redis 8.0.2 (AOF) — Full Command Benchmark

**Date:** $(date -Iseconds)
**Platform:** $(uname -srm)
**Redis:** 8.0.2, appendonly=yes, appendfsync=everysec
**Moon:** v0.1.0, 1 shard, monoio io_uring, per-shard WAL everysec

---

HDR

# ══════════════════════════════════════════════════════════════
# KV BENCHMARKS
# ══════════════════════════════════════════════════════════════

for section in "p=1|1|50|100000" "p=16|16|50|200000" "p=64|64|100|1000000"; do
    IFS='|' read -r title pipeline clients n <<< "$section"
    [ "$pipeline" = "64" ] && cmds="get set" || cmds="get set incr lpush rpush lpop rpop sadd spop hset zadd"

    echo "## KV: $title (c=$clients, n=$n)" >> "$OUT"
    echo "" >> "$OUT"
    echo "| Command | Redis(AOF) | Moon | Moon/Redis |" >> "$OUT"
    echo "|---------|----------:|-----:|:----------:|" >> "$OUT"

    for cmd in $cmds; do
        log "$cmd $title"
        r=$(redis_rps "$cmd" "$pipeline" "$clients" "$n")
        m=$(moon_rps "$cmd" "$pipeline" "$clients" "$n")
        rt=$(calc_ratio "$m" "$r")
        CMD_UP=$(echo "$cmd" | tr 'a-z' 'A-Z')
        printf "| %-7s | %s | %s | %sx |\n" "$CMD_UP" "${r:---}" "${m:---}" "$rt" >> "$OUT"
    done
    echo "" >> "$OUT"
done

# ══════════════════════════════════════════════════════════════
# VECTOR BENCHMARKS (Moon vs Qdrant)
# ══════════════════════════════════════════════════════════════

if [ "$QDRANT_OK" = true ]; then
    echo "## Vector: Moon vs Qdrant (128d, 10K vectors, k=10)" >> "$OUT"
    echo "" >> "$OUT"

    # Create Qdrant collection
    curl -sf -X PUT "http://localhost:6333/collections/bench" \
        -H "Content-Type: application/json" \
        -d '{"vectors":{"size":128,"distance":"Cosine"}}' > /dev/null

    # Create Moon index
    redis-cli -p 6400 FT.CREATE bench_idx ON HASH PREFIX 1 vec: \
        SCHEMA embedding VECTOR FLAT 6 DIM 128 DISTANCE_METRIC COSINE TYPE FLOAT32 > /dev/null 2>&1

    # ── Insert into Qdrant (batches of 100) ──
    log "Inserting 10K vectors into Qdrant..."
    QI_START=$SECONDS
    for bs in $(seq 0 100 9900); do
        pts=$(python3 -c "
import random, json
pts = []
for i in range($bs, $bs+100):
    random.seed(i)
    v = [round(random.gauss(0,1),4) for _ in range(128)]
    pts.append({'id':i,'vector':v})
print(json.dumps({'points':pts}))
")
        curl -sf -X PUT "http://localhost:6333/collections/bench/points" \
            -H "Content-Type: application/json" -d "$pts" > /dev/null
    done
    QI_SEC=$((SECONDS - QI_START))
    log "Qdrant insert: ${QI_SEC}s"

    # ── Insert into Moon (pipeline for speed) ──
    log "Inserting 10K vectors into Moon..."
    MI_START=$SECONDS
    python3 -c "
import struct, random, socket
s = socket.socket()
s.connect(('127.0.0.1', 6400))
for i in range(10000):
    random.seed(i)
    v = [random.gauss(0,1) for _ in range(128)]
    blob = struct.pack('128f', *v).hex()
    cmd = f'*4\r\n\$4\r\nHSET\r\n\${len(f\"vec:{i}\")}\r\nvec:{i}\r\n\$9\r\nembedding\r\n\${len(blob)}\r\n{blob}\r\n'
    s.sendall(cmd.encode())
# Drain replies
import time; time.sleep(0.5)
s.close()
" 2>/dev/null
    MI_SEC=$((SECONDS - MI_START))
    log "Moon insert: ${MI_SEC}s"

    # ── Query Qdrant ──
    log "Querying Qdrant 100x..."
    QQ_START=$SECONDS
    for q in $(seq 0 99); do
        qv=$(python3 -c "import random,json; random.seed($q+50000); print(json.dumps([round(random.gauss(0,1),4) for _ in range(128)]))")
        curl -sf -X POST "http://localhost:6333/collections/bench/points/search" \
            -H "Content-Type: application/json" \
            -d "{\"vector\":$qv,\"limit\":10}" > /dev/null
    done
    QQ_SEC=$((SECONDS - QQ_START))
    QQ_QPS=$((100 / (QQ_SEC > 0 ? QQ_SEC : 1)))

    # ── Query Moon ──
    log "Querying Moon 100x..."
    MQ_START=$SECONDS
    for q in $(seq 0 99); do
        qblob=$(python3 -c "
import struct,random
random.seed($q+50000)
v=[random.gauss(0,1) for _ in range(128)]
import sys; sys.stdout.buffer.write(struct.pack('128f',*v))
" | xxd -p | tr -d '\n')
        redis-cli -p 6400 FT.SEARCH bench_idx "*=>[KNN 10 @embedding \$BLOB AS score]" PARAMS 2 BLOB "$qblob" DIALECT 2 > /dev/null 2>&1
    done
    MQ_SEC=$((SECONDS - MQ_START))
    MQ_QPS=$((100 / (MQ_SEC > 0 ? MQ_SEC : 1)))

    cat >> "$OUT" <<VTAB
| Metric | Moon | Qdrant | Moon/Qdrant |
|--------|-----:|-------:|:-----------:|
| Insert 10K (128d) | ${MI_SEC}s | ${QI_SEC}s | $(calc_ratio "$QI_SEC" "$MI_SEC")x faster |
| Search QPS (k=10) | ~${MQ_QPS} | ~${QQ_QPS} | $(calc_ratio "$MQ_QPS" "$QQ_QPS")x |

VTAB
else
    echo "## Vector: Qdrant not available — skipped" >> "$OUT"
    echo "" >> "$OUT"
fi

# ── Cleanup ───────────────────────────────────────────────────
kill $MOON_PID 2>/dev/null || true
kill $QDRANT_PID 2>/dev/null || true
redis-cli -p 6399 SHUTDOWN NOSAVE 2>/dev/null || true
rm -rf /tmp/redis-aof /tmp/qdrant-data 2>/dev/null || true

echo "=== DONE ===" >> "$OUT"
cat "$OUT"
