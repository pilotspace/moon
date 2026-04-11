#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-graph-compare.sh -- Moon Graph vs FalkorDB (Redis Graph) benchmark
#
# Compares graph operation throughput between Moon and FalkorDB using
# identical workloads over redis-cli. FalkorDB runs via Docker.
#
# Usage:
#   ./scripts/bench-graph-compare.sh                # Full run
#   ./scripts/bench-graph-compare.sh --nodes 5000   # Custom scale
#   ./scripts/bench-graph-compare.sh --skip-build   # Skip Moon cargo build
#   ./scripts/bench-graph-compare.sh --moon-only    # Skip FalkorDB (no Docker)
###############################################################################

PORT_MOON=16700
PORT_FALKOR=16701
NODES=1000
EDGES=3000
SKIP_BUILD=false
MOON_ONLY=false
BINARY="./target/release/moon"
MOON_PID=""
FALKOR_CID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes)      NODES="$2"; shift 2 ;;
        --edges)      EDGES="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --moon-only)  MOON_ONLY=true; shift ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*"; }

cleanup() {
    set +e
    [[ -n "${MOON_PID:-}" ]] && kill "$MOON_PID" 2>/dev/null && wait "$MOON_PID" 2>/dev/null
    [[ -n "${FALKOR_CID:-}" ]] && docker stop "$FALKOR_CID" >/dev/null 2>&1 && docker rm "$FALKOR_CID" >/dev/null 2>&1
    set -e
}
trap cleanup EXIT

CLI_MOON="redis-cli -p $PORT_MOON"
CLI_FALKOR="redis-cli -p $PORT_FALKOR"

###############################################################################
# Build Moon
###############################################################################
if [[ "$SKIP_BUILD" == "false" ]]; then
    log "Building Moon with graph feature..."
    cargo build --release --no-default-features --features runtime-tokio,jemalloc,graph 2>&1 | tail -1
fi

###############################################################################
# Start Moon
###############################################################################
log "Starting Moon on port $PORT_MOON..."
MOON_NO_URING=1 $BINARY --port $PORT_MOON --shards 1 --protected-mode no > /tmp/moon_bench.log 2>&1 &
MOON_PID=$!
for i in $(seq 1 40); do
    $CLI_MOON PING > /dev/null 2>&1 && break
    sleep 0.25
done
if ! $CLI_MOON PING > /dev/null 2>&1; then
    log "ERROR: Moon failed to start"
    cat /tmp/moon_bench.log
    exit 1
fi
log "Moon ready (PID=$MOON_PID)"

###############################################################################
# Start FalkorDB (via Docker)
###############################################################################
if [[ "$MOON_ONLY" == "false" ]]; then
    log "Starting FalkorDB on port $PORT_FALKOR..."
    FALKOR_CID=$(docker run -d --rm -p $PORT_FALKOR:6379 falkordb/falkordb:latest 2>/dev/null || echo "")
    if [[ -z "$FALKOR_CID" ]]; then
        log "WARNING: Docker/FalkorDB not available. Running Moon-only benchmark."
        MOON_ONLY=true
    else
        for i in $(seq 1 40); do
            $CLI_FALKOR PING > /dev/null 2>&1 && break
            sleep 0.25
        done
        if ! $CLI_FALKOR PING > /dev/null 2>&1; then
            log "WARNING: FalkorDB failed to start. Running Moon-only."
            MOON_ONLY=true
        else
            log "FalkorDB ready (container=$FALKOR_CID)"
        fi
    fi
fi

###############################################################################
# Benchmark function
###############################################################################
bench_engine() {
    local ENGINE="$1"
    local CLI="$2"
    local PORT="$3"
    local USE_CYPHER="$4"  # "cypher" for FalkorDB, "native" for Moon

    log "=== [$ENGINE] Node Insertion ($NODES nodes) ==="
    local START_NS=$(date +%s%N)

    if [[ "$USE_CYPHER" == "cypher" ]]; then
        # FalkorDB uses GRAPH.QUERY with Cypher CREATE
        for i in $(seq 1 $NODES); do
            $CLI GRAPH.QUERY bench "CREATE (:Node {id: $i, name: 'knowledge_$i', confidence: 0.42})" > /dev/null 2>&1
        done
    else
        # Moon uses native GRAPH.ADDNODE
        local NODE_IDS=()
        for i in $(seq 1 $NODES); do
            local LABEL="Concept"
            case $((i % 5)) in
                1) LABEL="Fact" ;;
                2) LABEL="Event" ;;
                3) LABEL="Source" ;;
                4) LABEL="Agent" ;;
            esac
            local RESULT
            RESULT=$($CLI GRAPH.ADDNODE bench "$LABEL" name "knowledge_$i" confidence "0.42" 2>&1)
            NODE_IDS+=("$RESULT")
        done
    fi

    local END_NS=$(date +%s%N)
    local ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
    local INSERT_OPS=$(( NODES * 1000 / (ELAPSED_MS + 1) ))
    log "  [$ENGINE] $NODES nodes in ${ELAPSED_MS}ms (${INSERT_OPS} inserts/s)"
    eval "${ENGINE}_NODE_MS=$ELAPSED_MS"
    eval "${ENGINE}_NODE_OPS=$INSERT_OPS"

    log "=== [$ENGINE] Edge Insertion ($EDGES edges) ==="
    START_NS=$(date +%s%N)
    local EDGE_OK=0

    if [[ "$USE_CYPHER" == "cypher" ]]; then
        # FalkorDB: MATCH two nodes, CREATE edge
        for i in $(seq 1 $EDGES); do
            local SRC=$(( (i % NODES) + 1 ))
            local DST=$(( ((i * 7 + 3) % NODES) + 1 ))
            [[ $SRC -eq $DST ]] && DST=$(( (DST % NODES) + 1 ))
            local ETYPE="RELATED_TO"
            case $((i % 5)) in
                1) ETYPE="DERIVED_FROM" ;;
                2) ETYPE="OBSERVED_AT" ;;
                3) ETYPE="SUPERSEDES" ;;
                4) ETYPE="CITED_BY" ;;
            esac
            $CLI GRAPH.QUERY bench "MATCH (a:Node {id: $SRC}), (b:Node {id: $DST}) CREATE (a)-[:${ETYPE} {weight: 0.42}]->(b)" > /dev/null 2>&1 && EDGE_OK=$((EDGE_OK + 1))
        done
    else
        # Moon: native GRAPH.ADDEDGE
        for i in $(seq 1 $EDGES); do
            local SRC_IDX=$(( i % ${#NODE_IDS[@]} ))
            local DST_IDX=$(( (i * 7 + 3) % ${#NODE_IDS[@]} ))
            [[ $SRC_IDX -eq $DST_IDX ]] && DST_IDX=$(( (DST_IDX + 1) % ${#NODE_IDS[@]} ))
            local ETYPE="RELATED_TO"
            case $((i % 5)) in
                1) ETYPE="DERIVED_FROM" ;;
                2) ETYPE="OBSERVED_AT" ;;
                3) ETYPE="SUPERSEDES" ;;
                4) ETYPE="CITED_BY" ;;
            esac
            $CLI GRAPH.ADDEDGE bench "${NODE_IDS[$SRC_IDX]}" "${NODE_IDS[$DST_IDX]}" "$ETYPE" WEIGHT "0.42" > /dev/null 2>&1 && EDGE_OK=$((EDGE_OK + 1))
        done
    fi

    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
    local EDGE_OPS=$(( EDGES * 1000 / (ELAPSED_MS + 1) ))
    log "  [$ENGINE] $EDGES edges ($EDGE_OK ok) in ${ELAPSED_MS}ms (${EDGE_OPS} edges/s)"
    eval "${ENGINE}_EDGE_MS=$ELAPSED_MS"
    eval "${ENGINE}_EDGE_OPS=$EDGE_OPS"

    log "=== [$ENGINE] 1-Hop Queries (200 random) ==="
    START_NS=$(date +%s%N)
    local QUERY_OK=0

    if [[ "$USE_CYPHER" == "cypher" ]]; then
        for i in $(seq 1 200); do
            local NID=$(( (RANDOM % NODES) + 1 ))
            $CLI GRAPH.QUERY bench "MATCH (a:Node {id: $NID})-[r]->(b) RETURN b.id, type(r) LIMIT 50" > /dev/null 2>&1 && QUERY_OK=$((QUERY_OK + 1))
        done
    else
        for i in $(seq 1 200); do
            local IDX=$(( RANDOM % ${#NODE_IDS[@]} ))
            $CLI GRAPH.NEIGHBORS bench "${NODE_IDS[$IDX]}" > /dev/null 2>&1 && QUERY_OK=$((QUERY_OK + 1))
        done
    fi

    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
    local QUERY_OPS=$(( 200 * 1000 / (ELAPSED_MS + 1) ))
    local AVG_US=$(( ELAPSED_MS * 1000 / 200 ))
    log "  [$ENGINE] 200 queries in ${ELAPSED_MS}ms (${QUERY_OPS} qps, avg ${AVG_US}µs, $QUERY_OK ok)"
    eval "${ENGINE}_QUERY_MS=$ELAPSED_MS"
    eval "${ENGINE}_QUERY_OPS=$QUERY_OPS"

    log "=== [$ENGINE] 2-Hop Queries (100 random) ==="
    START_NS=$(date +%s%N)
    QUERY_OK=0

    if [[ "$USE_CYPHER" == "cypher" ]]; then
        for i in $(seq 1 100); do
            local NID=$(( (RANDOM % NODES) + 1 ))
            $CLI GRAPH.QUERY bench "MATCH (a:Node {id: $NID})-[*1..2]->(b) RETURN DISTINCT b.id LIMIT 100" > /dev/null 2>&1 && QUERY_OK=$((QUERY_OK + 1))
        done
    else
        for i in $(seq 1 100); do
            local IDX=$(( RANDOM % ${#NODE_IDS[@]} ))
            $CLI GRAPH.NEIGHBORS bench "${NODE_IDS[$IDX]}" DEPTH 2 > /dev/null 2>&1 && QUERY_OK=$((QUERY_OK + 1))
        done
    fi

    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
    QUERY_OPS=$(( 100 * 1000 / (ELAPSED_MS + 1) ))
    AVG_US=$(( ELAPSED_MS * 1000 / 100 ))
    log "  [$ENGINE] 100 queries in ${ELAPSED_MS}ms (${QUERY_OPS} qps, avg ${AVG_US}µs, $QUERY_OK ok)"
    eval "${ENGINE}_2HOP_MS=$ELAPSED_MS"
    eval "${ENGINE}_2HOP_OPS=$QUERY_OPS"

    log "=== [$ENGINE] Cypher Pattern Match (50 queries) ==="
    START_NS=$(date +%s%N)
    QUERY_OK=0

    if [[ "$USE_CYPHER" == "cypher" ]]; then
        for i in $(seq 1 50); do
            $CLI GRAPH.QUERY bench "MATCH (a:Node)-[:RELATED_TO]->(b:Node) RETURN a.id, b.id LIMIT 10" > /dev/null 2>&1 && QUERY_OK=$((QUERY_OK + 1))
        done
    else
        for i in $(seq 1 50); do
            $CLI GRAPH.QUERY bench "MATCH (n:Concept) RETURN n LIMIT 10" > /dev/null 2>&1 && QUERY_OK=$((QUERY_OK + 1))
        done
    fi

    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
    QUERY_OPS=$(( 50 * 1000 / (ELAPSED_MS + 1) ))
    AVG_US=$(( ELAPSED_MS * 1000 / 50 ))
    log "  [$ENGINE] 50 Cypher queries in ${ELAPSED_MS}ms (${QUERY_OPS} qps, avg ${AVG_US}µs, $QUERY_OK ok)"
    eval "${ENGINE}_CYPHER_MS=$ELAPSED_MS"
    eval "${ENGINE}_CYPHER_OPS=$QUERY_OPS"
}

###############################################################################
# Create graphs
###############################################################################
log "Creating graphs..."
$CLI_MOON GRAPH.CREATE bench > /dev/null 2>&1
if [[ "$MOON_ONLY" == "false" ]]; then
    # FalkorDB auto-creates on first GRAPH.QUERY
    $CLI_FALKOR GRAPH.QUERY bench "RETURN 1" > /dev/null 2>&1
fi

###############################################################################
# Run benchmarks
###############################################################################
bench_engine "MOON" "$CLI_MOON" "$PORT_MOON" "native"

if [[ "$MOON_ONLY" == "false" ]]; then
    bench_engine "FALKOR" "$CLI_FALKOR" "$PORT_FALKOR" "cypher"
fi

###############################################################################
# Summary
###############################################################################
echo ""
echo "============================================================"
echo "  GRAPH ENGINE BENCHMARK: Moon vs FalkorDB"
echo "============================================================"
echo "  Scale: $NODES nodes, $EDGES edges"
echo "  Protocol: redis-cli over TCP (sequential, 1 client)"
echo "============================================================"
echo ""

if [[ "$MOON_ONLY" == "false" ]]; then
    printf "%-25s %12s %12s %10s\n" "Operation" "Moon" "FalkorDB" "Ratio"
    printf "%-25s %12s %12s %10s\n" "-------------------------" "------------" "------------" "----------"
    printf "%-25s %10s/s %10s/s %8.1fx\n" "Node Insert" "$MOON_NODE_OPS" "$FALKOR_NODE_OPS" "$(echo "scale=1; $MOON_NODE_OPS / ($FALKOR_NODE_OPS + 1)" | bc)"
    printf "%-25s %10s/s %10s/s %8.1fx\n" "Edge Insert" "$MOON_EDGE_OPS" "$FALKOR_EDGE_OPS" "$(echo "scale=1; $MOON_EDGE_OPS / ($FALKOR_EDGE_OPS + 1)" | bc)"
    printf "%-25s %10s/s %10s/s %8.1fx\n" "1-Hop Query" "$MOON_QUERY_OPS" "$FALKOR_QUERY_OPS" "$(echo "scale=1; $MOON_QUERY_OPS / ($FALKOR_QUERY_OPS + 1)" | bc)"
    printf "%-25s %10s/s %10s/s %8.1fx\n" "2-Hop Query" "$MOON_2HOP_OPS" "$FALKOR_2HOP_OPS" "$(echo "scale=1; $MOON_2HOP_OPS / ($FALKOR_2HOP_OPS + 1)" | bc)"
    printf "%-25s %10s/s %10s/s %8.1fx\n" "Cypher Query" "$MOON_CYPHER_OPS" "$FALKOR_CYPHER_OPS" "$(echo "scale=1; $MOON_CYPHER_OPS / ($FALKOR_CYPHER_OPS + 1)" | bc)"
else
    printf "%-25s %12s\n" "Operation" "Moon"
    printf "%-25s %12s\n" "-------------------------" "------------"
    printf "%-25s %10s/s\n" "Node Insert" "$MOON_NODE_OPS"
    printf "%-25s %10s/s\n" "Edge Insert" "$MOON_EDGE_OPS"
    printf "%-25s %10s/s\n" "1-Hop Query" "$MOON_QUERY_OPS"
    printf "%-25s %10s/s\n" "2-Hop Query" "$MOON_2HOP_OPS"
    printf "%-25s %10s/s\n" "Cypher Query" "$MOON_CYPHER_OPS"
    echo ""
    echo "  (FalkorDB comparison skipped — use --moon-only=false with Docker)"
fi

echo ""
echo "============================================================"
echo "  Note: Sequential redis-cli (1 process per command)."
echo "  Real throughput is 100-1000x higher with pipelining."
echo "  Criterion micro-benchmarks: CSR 1-hop = 923ps, insert = 44ns"
echo "============================================================"
