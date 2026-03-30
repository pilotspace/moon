#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# profile-vector.sh -- Generate flamegraph for HNSW search hot path
#
# Prerequisites:
#   cargo install flamegraph    (for --tool flamegraph, default)
#   brew install samply          (for --tool samply on macOS)
#   linux-perf-tools             (for flamegraph on Linux)
#   dtrace                       (built-in on macOS, used by flamegraph)
#
# Usage:
#   ./scripts/profile-vector.sh                        # Default: 768d search
#   ./scripts/profile-vector.sh --filter hnsw_build    # Profile build path
#   ./scripts/profile-vector.sh --filter hnsw_search   # Profile 128d search
#   ./scripts/profile-vector.sh --tool samply           # Use samply profiler
#   ./scripts/profile-vector.sh --help                  # Show usage
#
# Known hotspots to look for (from Phase 59-69 Criterion data):
#   1. TQ/SQ distance computation (l2_i8, ADC table lookup) -- expected dominant
#   2. HNSW graph traversal (neighbor loading, L1/L2 cache misses on layer-0)
#   3. FWHT transform during TQ encoding (encode_tq_mse)
#   4. Binary heap operations in search priority queue (BinaryHeap push/pop)
#   5. SmallVec overflow in upper HNSW layers (M=16 connections per node)
#   6. BitVec test_and_set for visited tracking (cache-line contention at scale)
#
# Optimization targets:
#   - Scalar fallback in SQ encode (should be SIMD-dispatched)
#   - SmallVec reallocation in upper HNSW layers (pre-size to max_level*M)
#   - Unnecessary norm re-computation (cache in TQ code metadata)
#   - BFS reorder effectiveness (measure cache miss ratio before/after)
###############################################################################

# ── Configuration ──────────────────────────────────────────────────────

BENCH_FILTER="hnsw_search_768d"
OUTPUT_DIR="target/flamegraph"
TOOL="flamegraph"   # "flamegraph" or "samply"
BENCH_NAME="hnsw_bench"

# ── Argument parsing ──────────────────────────────────────────────────

usage() {
    cat <<'USAGE'
profile-vector.sh -- Generate flamegraph for HNSW search hot path

OPTIONS:
  --filter PATTERN   Criterion benchmark filter (default: hnsw_search_768d)
  --tool TOOL        Profiling tool: flamegraph or samply (default: flamegraph)
  --output-dir DIR   Output directory for SVG files (default: target/flamegraph)
  --bench NAME       Criterion bench target name (default: hnsw_bench)
  --help             Show this help

EXAMPLES:
  ./scripts/profile-vector.sh                             # 768d search flamegraph
  ./scripts/profile-vector.sh --filter hnsw_build_768d    # 768d build flamegraph
  ./scripts/profile-vector.sh --filter hnsw_search_ef     # ef sweep flamegraph
  ./scripts/profile-vector.sh --tool samply                # Use samply profiler

KNOWN HOTSPOTS:
  1. TQ/SQ distance computation (l2_i8, ADC lookup) -- expected dominant
  2. HNSW neighbor traversal (layer-0 cache misses)
  3. FWHT transform in TQ encoding
  4. BinaryHeap operations in search priority queue
  5. SmallVec overflow in upper HNSW layers
  6. BitVec visited tracking (cache-line access pattern)
USAGE
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --filter)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --filter requires a pattern"; exit 1
            fi
            BENCH_FILTER="$2"; shift 2 ;;
        --tool)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --tool requires 'flamegraph' or 'samply'"; exit 1
            fi
            TOOL="$2"; shift 2 ;;
        --output-dir)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --output-dir requires a directory path"; exit 1
            fi
            OUTPUT_DIR="$2"; shift 2 ;;
        --bench)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --bench requires a bench target name"; exit 1
            fi
            BENCH_NAME="$2"; shift 2 ;;
        --help|-h)
            usage ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Helpers ────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

# ── Validate prerequisites ─────────────────────────────────────────────

if [[ "$TOOL" == "flamegraph" ]]; then
    if ! command -v cargo-flamegraph &>/dev/null && ! cargo flamegraph --help &>/dev/null 2>&1; then
        echo "Error: cargo-flamegraph not found. Install with: cargo install flamegraph"
        exit 1
    fi
elif [[ "$TOOL" == "samply" ]]; then
    if ! command -v samply &>/dev/null; then
        echo "Error: samply not found. Install with: brew install samply (macOS) or cargo install samply"
        exit 1
    fi
else
    echo "Error: unknown tool '$TOOL'. Use 'flamegraph' or 'samply'."
    exit 1
fi

# ── Build benchmarks ──────────────────────────────────────────────────

log "Building benchmarks in release mode..."
cargo bench --bench "$BENCH_NAME" --no-run 2>&1 | tail -5

# Find the benchmark binary
BENCH_BIN=$(find target/release/deps -name "${BENCH_NAME}-*" -type f -perm +111 2>/dev/null | head -1)
if [[ -z "$BENCH_BIN" ]]; then
    log "Error: could not find benchmark binary for '$BENCH_NAME'"
    exit 1
fi
log "Found benchmark binary: $BENCH_BIN"

# ── Create output directory ────────────────────────────────────────────

mkdir -p "$OUTPUT_DIR"

# ── Profile ────────────────────────────────────────────────────────────

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SAFE_FILTER=$(echo "$BENCH_FILTER" | tr '/' '-')

if [[ "$TOOL" == "flamegraph" ]]; then
    OUTPUT_SVG="$OUTPUT_DIR/hnsw-${SAFE_FILTER}-${TIMESTAMP}.svg"
    log "Generating flamegraph for '$BENCH_FILTER'..."
    log "Output: $OUTPUT_SVG"

    # Run cargo flamegraph on the bench binary
    # --bench flag tells cargo flamegraph to use the benchmark target
    # The -- after bench name passes arguments to the criterion binary
    cargo flamegraph \
        --bench "$BENCH_NAME" \
        --output "$OUTPUT_SVG" \
        -- --bench "$BENCH_FILTER" \
        2>&1 | tail -10

    if [[ -f "$OUTPUT_SVG" ]]; then
        log "Flamegraph saved to: $OUTPUT_SVG"
        log ""
        log "=== Analysis Guide ==="
        log "Look for these hot functions (sorted by expected contribution):"
        log "  1. distance::*::l2_*         -- Distance computation (should be SIMD)"
        log "  2. turbo_quant::*::adc_*     -- ADC table lookup for TQ distances"
        log "  3. hnsw::search::hnsw_search -- Graph traversal + neighbor loading"
        log "  4. BinaryHeap::*             -- Priority queue operations"
        log "  5. turbo_quant::fwht::*      -- FWHT transform (query encoding)"
        log "  6. BitVec::test_and_set      -- Visited tracking"
        log ""
        log "Optimization signals:"
        log "  - If scalar:: functions appear instead of simd:: -> dispatch not working"
        log "  - If alloc:: functions visible -> unexpected heap allocation on hot path"
        log "  - If memcpy visible -> unnecessary data copying (should use slices)"
        log ""

        # Open in browser on macOS
        if [[ "$(uname -s)" == "Darwin" ]]; then
            log "Opening flamegraph in browser..."
            open "$OUTPUT_SVG" 2>/dev/null || true
        fi
    else
        log "WARNING: Flamegraph SVG not generated. Check cargo-flamegraph output above."
    fi

elif [[ "$TOOL" == "samply" ]]; then
    log "Starting samply profiler for '$BENCH_FILTER'..."
    log "Samply will open its web UI automatically."
    log ""
    log "After profiling, look for the same hotspots listed in --help output."

    samply record -- "$BENCH_BIN" --bench "$BENCH_FILTER"
fi

log "Done."
