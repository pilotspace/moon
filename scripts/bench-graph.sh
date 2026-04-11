#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-graph.sh -- Graph Engine E2E benchmark with realistic AI agent data
#
# Usage:
#   ./scripts/bench-graph.sh                # Full run (1K nodes, 5K edges)
#   ./scripts/bench-graph.sh --nodes 10000  # Custom node count
#   ./scripts/bench-graph.sh --edges 50000  # Custom edge count
#   ./scripts/bench-graph.sh --skip-build   # Skip cargo build
###############################################################################

PORT=6501
NODES=1000
EDGES=5000
SHARDS=1
SKIP_BUILD=false
BINARY="./target/release/moon"
MOON_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes)   NODES="$2"; shift 2 ;;
        --edges)   EDGES="$2"; shift 2 ;;
        --shards)  SHARDS="$2"; shift 2 ;;
        --port)    PORT="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*"; }
err() { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; }

cleanup() {
    set +e
    if [[ -n "${MOON_PID:-}" ]]; then
        kill "$MOON_PID" 2>/dev/null
        wait "$MOON_PID" 2>/dev/null
    fi
    set -e
}
trap cleanup EXIT

CLI="redis-cli -p $PORT"

###############################################################################
# 1. Build
###############################################################################
if [[ "$SKIP_BUILD" == "false" ]]; then
    log "Building Moon with graph feature..."
    cargo build --release --no-default-features --features runtime-tokio,jemalloc,graph 2>&1 | tail -1
fi

###############################################################################
# 2. Start Moon
###############################################################################
log "Starting Moon on port $PORT (shards=$SHARDS)..."
MOON_NO_URING=1 $BINARY --port $PORT --shards $SHARDS --protected-mode no > /tmp/moon_graph_bench.log 2>&1 &
MOON_PID=$!

# Wait for server to be ready (poll up to 10 seconds)
for attempt in $(seq 1 20); do
    if $CLI PING > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

if ! $CLI PING > /dev/null 2>&1; then
    err "Moon failed to start after 10s. Log:"
    cat /tmp/moon_graph_bench.log
    exit 1
fi
log "Moon started (PID=$MOON_PID)"

###############################################################################
# 3. AI Agent Knowledge Graph — Realistic Mock Data
###############################################################################
GRAPH="{agent-bench}:knowledge"

log "=== Phase 1: Graph Creation ==="
set +e
RESULT=$($CLI GRAPH.CREATE "$GRAPH" 2>&1)
RC=$?
set -e
echo "  GRAPH.CREATE: $RESULT (rc=$RC)"
if [[ "$RESULT" == *"ERR"* ]]; then
    err "Graph creation failed. Aborting."
    exit 1
fi

log "=== Phase 2: Node Insertion ($NODES nodes) ==="
NODE_IDS=()
LABELS=("Concept" "Fact" "Event" "Source" "Agent")
START_NS=$(date +%s%N)

for i in $(seq 1 $NODES); do
    LABEL=${LABELS[$((i % 5))]}
    set +e
    RESULT=$($CLI GRAPH.ADDNODE "$GRAPH" "$LABEL" name "knowledge_$i" confidence "0.42" created_at "$((1712800000 + i))" 2>&1)
    set -e
    if [[ "$RESULT" == *"ERR"* ]]; then
        if [[ $i -le 3 ]]; then err "  ADDNODE $i failed: $RESULT"; fi
    else
        NODE_IDS+=("$RESULT")
    fi
done
ACTUAL_NODES=${#NODE_IDS[@]}
if [[ $ACTUAL_NODES -eq 0 ]]; then
    err "No nodes inserted. First result was: $RESULT"
    exit 1
fi

END_NS=$(date +%s%N)
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
OPS_PER_SEC=$(( NODES * 1000 / (ELAPSED_MS + 1) ))
log "  $NODES nodes inserted in ${ELAPSED_MS}ms (${OPS_PER_SEC} ops/s)"

log "=== Phase 3: Edge Insertion ($EDGES edges) ==="
EDGE_TYPES=("RELATED_TO" "DERIVED_FROM" "OBSERVED_AT" "SUPERSEDES" "CITED_BY")
START_NS=$(date +%s%N)

EDGE_OK=0
EDGE_ERR=0
for i in $(seq 1 $EDGES); do
    SRC_IDX=$(( RANDOM % ACTUAL_NODES ))
    DST_IDX=$(( RANDOM % ACTUAL_NODES ))
    # Avoid self-loops
    if [[ $DST_IDX -eq $SRC_IDX ]]; then
        DST_IDX=$(( (SRC_IDX + 1) % ACTUAL_NODES ))
    fi
    SRC_ID="${NODE_IDS[$SRC_IDX]}"
    DST_ID="${NODE_IDS[$DST_IDX]}"
    ETYPE=${EDGE_TYPES[$((i % 5))]}
    set +e
    RESULT=$($CLI GRAPH.ADDEDGE "$GRAPH" "$SRC_ID" "$DST_ID" "$ETYPE" WEIGHT "0.42" 2>&1)
    set -e
    if [[ "$RESULT" == *"ERR"* ]]; then
        EDGE_ERR=$((EDGE_ERR + 1))
        if [[ $EDGE_ERR -le 3 ]]; then
            err "  Edge $i failed: $RESULT (src=$SRC_ID dst=$DST_ID)"
        fi
    else
        EDGE_OK=$((EDGE_OK + 1))
    fi
done
log "  $EDGE_OK edges ok, $EDGE_ERR errors"

END_NS=$(date +%s%N)
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
OPS_PER_SEC=$(( EDGES * 1000 / (ELAPSED_MS + 1) ))
log "  $EDGES edges inserted in ${ELAPSED_MS}ms (${OPS_PER_SEC} ops/s)"

log "=== Phase 4: Graph Info ==="
$CLI GRAPH.INFO "$GRAPH" 2>&1 | head -20

log "=== Phase 5: 1-Hop Neighbor Queries (100 random) ==="
START_NS=$(date +%s%N)
SUCCESS=0
EMPTY=0
for i in $(seq 1 100); do
    IDX=$(( RANDOM % NODES ))
    NID="${NODE_IDS[$IDX]}"
    RESULT=$($CLI GRAPH.NEIGHBORS "$GRAPH" "$NID" 2>&1)
    if [[ "$RESULT" != *"ERR"* ]]; then
        SUCCESS=$((SUCCESS + 1))
        if [[ "$RESULT" == "(empty"* ]] || [[ "$RESULT" == "" ]]; then
            EMPTY=$((EMPTY + 1))
        fi
    fi
done
END_NS=$(date +%s%N)
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
AVG_US=$(( ELAPSED_MS * 1000 / 100 ))
log "  100 queries in ${ELAPSED_MS}ms (avg ${AVG_US}µs/query, $SUCCESS ok, $EMPTY empty)"

log "=== Phase 6: 2-Hop Neighbor Queries (50 random) ==="
START_NS=$(date +%s%N)
SUCCESS=0
for i in $(seq 1 50); do
    IDX=$(( RANDOM % NODES ))
    NID="${NODE_IDS[$IDX]}"
    RESULT=$($CLI GRAPH.NEIGHBORS "$GRAPH" "$NID" DEPTH 2 2>&1)
    if [[ "$RESULT" != *"ERR"* ]]; then
        SUCCESS=$((SUCCESS + 1))
    fi
done
END_NS=$(date +%s%N)
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
AVG_US=$(( ELAPSED_MS * 1000 / 50 ))
log "  50 queries in ${ELAPSED_MS}ms (avg ${AVG_US}µs/query, $SUCCESS ok)"

log "=== Phase 7: Typed Edge Queries (50 random, TYPE RELATED_TO) ==="
START_NS=$(date +%s%N)
SUCCESS=0
for i in $(seq 1 50); do
    IDX=$(( RANDOM % NODES ))
    NID="${NODE_IDS[$IDX]}"
    RESULT=$($CLI GRAPH.NEIGHBORS "$GRAPH" "$NID" TYPE RELATED_TO 2>&1)
    if [[ "$RESULT" != *"ERR"* ]]; then
        SUCCESS=$((SUCCESS + 1))
    fi
done
END_NS=$(date +%s%N)
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
AVG_US=$(( ELAPSED_MS * 1000 / 50 ))
log "  50 queries in ${ELAPSED_MS}ms (avg ${AVG_US}µs/query, $SUCCESS ok)"

log "=== Phase 8: Cypher Query (MATCH + RETURN) ==="
START_NS=$(date +%s%N)
for i in $(seq 1 20); do
    RESULT=$($CLI GRAPH.QUERY "$GRAPH" "MATCH (n:Concept) RETURN n LIMIT 10" 2>&1)
done
END_NS=$(date +%s%N)
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
AVG_US=$(( ELAPSED_MS * 1000 / 20 ))
log "  20 Cypher queries in ${ELAPSED_MS}ms (avg ${AVG_US}µs/query)"

log "=== Phase 9: GRAPH.EXPLAIN (query plan) ==="
RESULT=$($CLI GRAPH.EXPLAIN "$GRAPH" "MATCH (n:Concept)-[:RELATED_TO]->(m) RETURN n, m LIMIT 10" 2>&1)
echo "  Plan: $(echo "$RESULT" | head -5)"

log "=== Phase 10: Graph List ==="
$CLI GRAPH.LIST 2>&1

log "=== Phase 11: KV Regression Check (1000 SET + 1000 GET) ==="
START_NS=$(date +%s%N)
for i in $(seq 1 1000); do
    $CLI SET "bench_key_$i" "value_$i" > /dev/null 2>&1
done
END_NS=$(date +%s%N)
SET_MS=$(( (END_NS - START_NS) / 1000000 ))

START_NS=$(date +%s%N)
for i in $(seq 1 1000); do
    $CLI GET "bench_key_$i" > /dev/null 2>&1
done
END_NS=$(date +%s%N)
GET_MS=$(( (END_NS - START_NS) / 1000000 ))

SET_OPS=$(( 1000 * 1000 / (SET_MS + 1) ))
GET_OPS=$(( 1000 * 1000 / (GET_MS + 1) ))
log "  SET: 1000 ops in ${SET_MS}ms (${SET_OPS} ops/s)"
log "  GET: 1000 ops in ${GET_MS}ms (${GET_OPS} ops/s)"

log "=== Phase 12: Graph Delete ==="
$CLI GRAPH.DELETE "$GRAPH" 2>&1
RESULT=$($CLI GRAPH.LIST 2>&1)
echo "  After delete: $RESULT"

###############################################################################
# Summary
###############################################################################
echo ""
echo "============================================================"
echo "  GRAPH ENGINE E2E BENCHMARK COMPLETE"
echo "============================================================"
echo "  Nodes: $NODES  |  Edges: $EDGES  |  Shards: $SHARDS"
echo "  Server: Moon with graph feature (tokio runtime)"
echo "============================================================"

cleanup
log "Done."
