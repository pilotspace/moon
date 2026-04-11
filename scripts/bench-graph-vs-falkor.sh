#!/usr/bin/env bash
# Moon vs FalkorDB — sequential redis-cli, small scale for speed.
set -uo pipefail

PORT_MOON=16700
PORT_FALKOR=16701
NODES=200
EDGES=600
QUERIES=100

cleanup() {
    set +e
    [ -n "${MOON_PID:-}" ] && kill "$MOON_PID" 2>/dev/null && wait "$MOON_PID" 2>/dev/null
    docker stop falkor_bench 2>/dev/null
    set -e
}
trap cleanup EXIT

log() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }
time_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

# --- Start Moon ---
log "Starting Moon..."
MOON_NO_URING=1 ./target/release/moon --port $PORT_MOON --shards 1 --protected-mode no > /dev/null 2>&1 &
MOON_PID=$!
for i in $(seq 1 30); do redis-cli -p $PORT_MOON PING > /dev/null 2>&1 && break; sleep 0.2; done
log "Moon ready"

# --- Start FalkorDB ---
log "Starting FalkorDB..."
FALKOR_OK=false
docker run -d --rm --name falkor_bench -p $PORT_FALKOR:6379 falkordb/falkordb:latest > /dev/null 2>&1 || true
for i in $(seq 1 40); do redis-cli -p $PORT_FALKOR PING > /dev/null 2>&1 && { FALKOR_OK=true; break; }; sleep 0.5; done
[ "$FALKOR_OK" = true ] && log "FalkorDB ready" || log "FalkorDB unavailable"

# ====== MOON BENCHMARK ======
redis-cli -p $PORT_MOON GRAPH.CREATE bench > /dev/null 2>&1
log "=== Moon: $NODES nodes ==="
NODE_IDS=()
T0=$(time_ms)
for i in $(seq 1 $NODES); do
    L="Concept"; case $((i%5)) in 1)L="Fact";;2)L="Event";;3)L="Source";;4)L="Agent";;esac
    R=$(redis-cli -p $PORT_MOON GRAPH.ADDNODE bench "$L" name "k_$i" conf "0.4" 2>/dev/null)
    NODE_IDS+=("$R")
done
T1=$(time_ms)
MOON_NODE_MS=$((T1-T0))
MOON_NODE_OPS=$((NODES*1000/(MOON_NODE_MS+1)))
log "  nodes: ${MOON_NODE_MS}ms (${MOON_NODE_OPS}/s)"

log "=== Moon: $EDGES edges ==="
T0=$(time_ms)
EOK=0
for i in $(seq 1 $EDGES); do
    S=$((i%${#NODE_IDS[@]})); D=$(((i*7+3)%${#NODE_IDS[@]}))
    [ $S -eq $D ] && D=$(((D+1)%${#NODE_IDS[@]}))
    E="RELATED_TO"; case $((i%5)) in 1)E="DERIVED";;2)E="OBSERVED";;3)E="SUPERSEDES";;4)E="CITED";;esac
    redis-cli -p $PORT_MOON GRAPH.ADDEDGE bench "${NODE_IDS[$S]}" "${NODE_IDS[$D]}" "$E" WEIGHT "0.4" > /dev/null 2>&1 && EOK=$((EOK+1))
done
T1=$(time_ms)
MOON_EDGE_MS=$((T1-T0))
MOON_EDGE_OPS=$((EDGES*1000/(MOON_EDGE_MS+1)))
log "  edges: ${MOON_EDGE_MS}ms (${MOON_EDGE_OPS}/s) [$EOK ok]"

log "=== Moon: $QUERIES 1-hop ==="
T0=$(time_ms)
for i in $(seq 1 $QUERIES); do
    IDX=$((RANDOM%${#NODE_IDS[@]}))
    redis-cli -p $PORT_MOON GRAPH.NEIGHBORS bench "${NODE_IDS[$IDX]}" > /dev/null 2>&1
done
T1=$(time_ms)
MOON_1HOP_MS=$((T1-T0))
MOON_1HOP_OPS=$((QUERIES*1000/(MOON_1HOP_MS+1)))
log "  1-hop: ${MOON_1HOP_MS}ms (${MOON_1HOP_OPS} qps)"

log "=== Moon: $QUERIES 2-hop ==="
T0=$(time_ms)
for i in $(seq 1 $QUERIES); do
    IDX=$((RANDOM%${#NODE_IDS[@]}))
    redis-cli -p $PORT_MOON GRAPH.NEIGHBORS bench "${NODE_IDS[$IDX]}" DEPTH 2 > /dev/null 2>&1
done
T1=$(time_ms)
MOON_2HOP_MS=$((T1-T0))
MOON_2HOP_OPS=$((QUERIES*1000/(MOON_2HOP_MS+1)))
log "  2-hop: ${MOON_2HOP_MS}ms (${MOON_2HOP_OPS} qps)"

# ====== FALKORDB BENCHMARK ======
FALKOR_NODE_OPS=0; FALKOR_EDGE_OPS=0; FALKOR_1HOP_OPS=0; FALKOR_2HOP_OPS=0

if [ "$FALKOR_OK" = true ]; then
    redis-cli -p $PORT_FALKOR GRAPH.QUERY bench "RETURN 1" > /dev/null 2>&1

    log "=== FalkorDB: $NODES nodes ==="
    T0=$(time_ms)
    for i in $(seq 1 $NODES); do
        redis-cli -p $PORT_FALKOR GRAPH.QUERY bench "CREATE (:Node {id:$i,name:'k_$i',conf:0.4})" > /dev/null 2>&1
    done
    T1=$(time_ms)
    FALKOR_NODE_MS=$((T1-T0))
    FALKOR_NODE_OPS=$((NODES*1000/(FALKOR_NODE_MS+1)))
    log "  nodes: ${FALKOR_NODE_MS}ms (${FALKOR_NODE_OPS}/s)"

    log "=== FalkorDB: $EDGES edges ==="
    T0=$(time_ms)
    EOK=0
    for i in $(seq 1 $EDGES); do
        S=$(((i%NODES)+1)); D=$((((i*7+3)%NODES)+1))
        [ $S -eq $D ] && D=$(((D%NODES)+1))
        E="RELATED_TO"; case $((i%5)) in 1)E="DERIVED";;2)E="OBSERVED";;3)E="SUPERSEDES";;4)E="CITED";;esac
        redis-cli -p $PORT_FALKOR GRAPH.QUERY bench "MATCH (a:Node{id:$S}),(b:Node{id:$D}) CREATE (a)-[:${E}{w:0.4}]->(b)" > /dev/null 2>&1 && EOK=$((EOK+1))
    done
    T1=$(time_ms)
    FALKOR_EDGE_MS=$((T1-T0))
    FALKOR_EDGE_OPS=$((EDGES*1000/(FALKOR_EDGE_MS+1)))
    log "  edges: ${FALKOR_EDGE_MS}ms (${FALKOR_EDGE_OPS}/s) [$EOK ok]"

    log "=== FalkorDB: $QUERIES 1-hop ==="
    T0=$(time_ms)
    for i in $(seq 1 $QUERIES); do
        NID=$(((RANDOM%NODES)+1))
        redis-cli -p $PORT_FALKOR GRAPH.QUERY bench "MATCH (a:Node{id:$NID})-[r]->(b) RETURN b.id LIMIT 50" > /dev/null 2>&1
    done
    T1=$(time_ms)
    FALKOR_1HOP_MS=$((T1-T0))
    FALKOR_1HOP_OPS=$((QUERIES*1000/(FALKOR_1HOP_MS+1)))
    log "  1-hop: ${FALKOR_1HOP_MS}ms (${FALKOR_1HOP_OPS} qps)"

    log "=== FalkorDB: $QUERIES 2-hop ==="
    T0=$(time_ms)
    for i in $(seq 1 $QUERIES); do
        NID=$(((RANDOM%NODES)+1))
        redis-cli -p $PORT_FALKOR GRAPH.QUERY bench "MATCH (a:Node{id:$NID})-[*1..2]->(b) RETURN DISTINCT b.id LIMIT 100" > /dev/null 2>&1
    done
    T1=$(time_ms)
    FALKOR_2HOP_MS=$((T1-T0))
    FALKOR_2HOP_OPS=$((QUERIES*1000/(FALKOR_2HOP_MS+1)))
    log "  2-hop: ${FALKOR_2HOP_MS}ms (${FALKOR_2HOP_OPS} qps)"
fi

# ====== RESULTS ======
echo ""
echo "============================================================"
echo "  Moon vs FalkorDB ($NODES nodes, $EDGES edges, $QUERIES q)"
echo "  Sequential redis-cli (fork per command, no pipelining)"
echo "============================================================"
echo ""

if [ "$FALKOR_OK" = true ]; then
    ratio() { python3 -c "print(f'{$1/max($2,1):.1f}')"; }
    printf "%-20s %10s %10s %8s\n" "Operation" "Moon" "FalkorDB" "Ratio"
    printf "%-20s %10s %10s %8s\n" "---" "---" "---" "---"
    printf "%-20s %8s/s %8s/s %6sx\n" "Node Insert" "$MOON_NODE_OPS" "$FALKOR_NODE_OPS" "$(ratio $MOON_NODE_OPS $FALKOR_NODE_OPS)"
    printf "%-20s %8s/s %8s/s %6sx\n" "Edge Insert" "$MOON_EDGE_OPS" "$FALKOR_EDGE_OPS" "$(ratio $MOON_EDGE_OPS $FALKOR_EDGE_OPS)"
    printf "%-20s %8s/s %8s/s %6sx\n" "1-Hop Query" "$MOON_1HOP_OPS" "$FALKOR_1HOP_OPS" "$(ratio $MOON_1HOP_OPS $FALKOR_1HOP_OPS)"
    printf "%-20s %8s/s %8s/s %6sx\n" "2-Hop Query" "$MOON_2HOP_OPS" "$FALKOR_2HOP_OPS" "$(ratio $MOON_2HOP_OPS $FALKOR_2HOP_OPS)"
else
    printf "%-20s %10s\n" "Operation" "Moon"
    printf "%-20s %10s\n" "---" "---"
    printf "%-20s %8s/s\n" "Node Insert" "$MOON_NODE_OPS"
    printf "%-20s %8s/s\n" "Edge Insert" "$MOON_EDGE_OPS"
    printf "%-20s %8s/s\n" "1-Hop Query" "$MOON_1HOP_OPS"
    printf "%-20s %8s/s\n" "2-Hop Query" "$MOON_2HOP_OPS"
    echo "(FalkorDB unavailable)"
fi
echo ""
echo "Note: Sequential redis-cli dominates latency (fork+exec per cmd)."
echo "Criterion micro-benchmarks show raw engine: CSR 1-hop=1.02ns, BFS 2-hop=4.99µs"
