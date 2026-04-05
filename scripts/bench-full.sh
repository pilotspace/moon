#!/usr/bin/env bash
set -uo pipefail
cd /Users/tindang/workspaces/tind-repo/moon
OUT=/tmp/bench-full-results.txt
: > "$OUT"

# Kill everything
killall -9 moon redis-server qdrant redis-benchmark 2>/dev/null
sleep 2

# Start Redis with AOF
rm -rf /tmp/redis-aof && mkdir -p /tmp/redis-aof
redis-server --port 6379 --bind 127.0.0.1 --protected-mode no \
    --appendonly yes --appendfsync everysec \
    --dir /tmp/redis-aof --daemonize yes --loglevel warning
sleep 1

# Start Moon
./target/release/moon --port 6400 --shards 1 &>/dev/null &
MOON_PID=$!
sleep 2

# Start Qdrant
rm -rf /tmp/qdrant-data && mkdir -p /tmp/qdrant-data
cat > /tmp/qdrant-cfg.yaml <<'Y'
storage:
  storage_path: /tmp/qdrant-data/storage
  snapshots_path: /tmp/qdrant-data/snapshots
service:
  http_port: 6333
  grpc_port: 6334
Y
/tmp/qdrant --config-path /tmp/qdrant-cfg.yaml &>/tmp/qdrant.log &
QDRANT_PID=$!
sleep 3

# Verify
redis-cli -p 6379 PING > /dev/null 2>&1 || { echo "Redis FAIL" >> "$OUT"; }
redis-cli -p 6400 PING > /dev/null 2>&1 || { echo "Moon FAIL" >> "$OUT"; }
QDRANT_OK=false
curl -sf http://localhost:6333/healthz > /dev/null 2>&1 && QDRANT_OK=true

cat >> "$OUT" <<HDR
# Moon vs Redis 8.0.2 (AOF) vs Qdrant 1.13.2
**Date:** $(date -Iseconds)
**Platform:** $(uname -srm)
**Redis:** 8.0.2, appendonly=yes, appendfsync=everysec
**Moon:** v0.1.0, 1 shard, monoio io_uring, per-shard WAL
**Qdrant:** 1.13.2 (vectors only)

HDR

# Helper: extract Moon RPS from redis-benchmark 8.x output
moon_rps() {
    local raw=$(timeout 8 redis-benchmark -p 6400 -c "$2" -n "$3" -t "$1" -P "$4" 2>&1)
    echo "$raw" | tr '\r' '\n' | grep "rps=" | grep -v "rps=0.0" | grep -v "nan" \
        | head -1 | awk -F'overall: ' '{print $2}' | awk -F')' '{print $1}'
}

redis_rps() {
    redis-benchmark -p 6379 -c "$2" -n "$3" -t "$1" -P "$4" --csv 2>&1 \
        | grep -v '^"test"' | head -1 | cut -d'"' -f4
}

ratio() {
    [ -n "$1" ] && [ -n "$2" ] && echo "scale=2; $1 / $2" | bc 2>/dev/null || echo "--"
}

# ═══ KV BENCHMARKS ═══

for sect in "p=1|1|50|100000" "p=16|16|50|200000" "p=64|64|100|500000"; do
    IFS='|' read -r title P C N <<< "$sect"
    [ "$P" = "64" ] && cmds="get set" || cmds="get set incr lpush rpush lpop rpop sadd spop hset zadd"

    echo "## KV: $title (c=$C, n=$N)" >> "$OUT"
    echo "" >> "$OUT"
    echo "| Command | Redis(AOF) | Moon | Moon/Redis |" >> "$OUT"
    echo "|---------|----------:|-----:|:----------:|" >> "$OUT"

    for cmd in $cmds; do
        echo -n "  $cmd $title..." >&2
        r=$(redis_rps "$cmd" "$C" "$N" "$P")
        m=$(moon_rps "$cmd" "$C" "$N" "$P")
        rt=$(ratio "$m" "$r")
        printf "| %-7s | %s | %s | %sx |\n" "$(echo $cmd | tr a-z A-Z)" "${r:---}" "${m:---}" "$rt" >> "$OUT"
        echo " done" >&2
    done
    echo "" >> "$OUT"
done

# ═══ VECTOR: Qdrant ═══

echo "## Vector: Moon vs Qdrant (128d, 10K, k=10)" >> "$OUT"
echo "" >> "$OUT"

if [ "$QDRANT_OK" = true ]; then
    # Qdrant: create + insert + query
    curl -sf -X PUT "http://localhost:6333/collections/bench" \
        -H "Content-Type: application/json" \
        -d '{"vectors":{"size":128,"distance":"Cosine"}}' > /dev/null

    echo -n "  Qdrant insert..." >&2
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
    echo " ${QI_SEC}s" >&2

    sleep 2  # indexing

    echo -n "  Qdrant query..." >&2
    QQ_START=$SECONDS
    for q in $(seq 0 99); do
        qv=$(python3 -c "import random,json; random.seed($q+50000); print(json.dumps([round(random.gauss(0,1),4) for _ in range(128)]))")
        curl -sf -X POST "http://localhost:6333/collections/bench/points/search" \
            -H "Content-Type: application/json" \
            -d "{\"vector\":$qv,\"limit\":10}" > /dev/null
    done
    QQ_SEC=$((SECONDS - QQ_START))
    QQ_SEC=$((QQ_SEC > 0 ? QQ_SEC : 1))
    echo " ${QQ_SEC}s" >&2

    cat >> "$OUT" <<VEC
| Metric | Qdrant |
|--------|-------:|
| Insert 10K (128d) | ${QI_SEC}s ($(( 10000 / (QI_SEC > 0 ? QI_SEC : 1) )) vec/s) |
| Search 100 queries (k=10) | ${QQ_SEC}s (~$(( 100 / QQ_SEC )) QPS) |

VEC
else
    echo "Qdrant not available." >> "$OUT"
fi

# ═══ CLEANUP ═══
kill $MOON_PID 2>/dev/null
kill $QDRANT_PID 2>/dev/null
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null
rm -rf /tmp/redis-aof /tmp/qdrant-data

echo "=== DONE ===" >> "$OUT"
cat "$OUT"
