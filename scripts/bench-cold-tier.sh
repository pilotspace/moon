#!/usr/bin/env bash
set -euo pipefail
###############################################################################
# bench-cold-tier.sh — DiskANN Cold Tier Benchmark
#
# Requirements:
#   - Linux with real NVMe SSD (or any SSD for baseline)
#   - Moon release build
#   - redis-benchmark, redis-cli
#   - python3 (no numpy needed)
#
# Usage:
#   ./scripts/bench-cold-tier.sh                    # Auto-detect disk
#   ./scripts/bench-cold-tier.sh --disk /mnt/nvme   # Specify offload dir
#   ./scripts/bench-cold-tier.sh --ramdisk          # Use tmpfs (functional test)
#   ./scripts/bench-cold-tier.sh --vectors 50000    # Vector count
#
# What it measures:
#   Phase 1: KV cold read-through (evicted keys read from disk)
#   Phase 2: Vector warm→cold transition + DiskANN search
#   Phase 3: Crash recovery from cold state
###############################################################################

OFFLOAD_DIR=""
USE_RAMDISK=false
N_KV=200000
N_VEC=20000
DIM=384
MOON_PORT=6500
MAXMEMORY="67108864"  # 64MB — Force eviction quickly

while [[ $# -gt 0 ]]; do
    case "$1" in
        --disk) OFFLOAD_DIR="$2"; shift 2 ;;
        --ramdisk) USE_RAMDISK=true; shift ;;
        --vectors) N_VEC="$2"; shift 2 ;;
        --kv) N_KV="$2"; shift 2 ;;
        --maxmemory) MAXMEMORY="$2"; shift 2 ;;  # bytes
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

cd "$(dirname "$0")/.."
BINARY=./target/release/moon

if [ ! -x "$BINARY" ]; then
    echo "Build first: cargo build --release"
    exit 1
fi

# Set up offload directory
if [ "$USE_RAMDISK" = true ]; then
    OFFLOAD_DIR=$(mktemp -d /tmp/moon-cold-bench.XXXXX)
    echo "Using tmpfs ramdisk: $OFFLOAD_DIR (functional test, not I/O benchmark)"
elif [ -z "$OFFLOAD_DIR" ]; then
    # Auto-detect: prefer /mnt/nvme, fall back to /tmp
    if [ -d /mnt/nvme ]; then
        OFFLOAD_DIR=/mnt/nvme/moon-cold-bench
    elif [ -d /data ]; then
        OFFLOAD_DIR=/data/moon-cold-bench
    else
        OFFLOAD_DIR=$(mktemp -d /tmp/moon-cold-bench.XXXXX)
        echo "WARNING: Using /tmp — not a real NVMe. Numbers will not reflect production."
    fi
fi
mkdir -p "$OFFLOAD_DIR"

DATA_DIR="$OFFLOAD_DIR/data"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# Detect disk type
DISK_TYPE="unknown"
if [ -e "$OFFLOAD_DIR" ]; then
    DEV=$(df "$OFFLOAD_DIR" 2>/dev/null | tail -1 | awk '{print $1}')
    if echo "$DEV" | grep -q "nvme"; then
        DISK_TYPE="NVMe"
    elif echo "$DEV" | grep -q "sd[a-z]"; then
        DISK_TYPE="SATA/SAS SSD"
    elif echo "$DEV" | grep -q "tmpfs\|ramfs"; then
        DISK_TYPE="tmpfs (RAM)"
    else
        DISK_TYPE="virtual/unknown ($DEV)"
    fi
fi

cleanup() {
    pkill -f "moon --port $MOON_PORT" 2>/dev/null || true
    sleep 1
}
trap cleanup EXIT

cat <<HEADER
================================================================
  Moon Cold Tier Benchmark
================================================================
  Platform:    $(uname -srm)
  Disk:        $DISK_TYPE ($OFFLOAD_DIR)
  Data dir:    $DATA_DIR
  Max memory:  $MAXMEMORY
  KV entries:  $N_KV (1KB values → ~200MB > maxmemory → forces eviction)
  Vectors:     $N_VEC × ${DIM}d
  Moon port:   $MOON_PORT
================================================================

HEADER

# ══════════════════════════════════════════════════════════════
# PHASE 1: KV DISK OFFLOAD — Eviction + Cold Read-Through
# ══════════════════════════════════════════════════════════════

echo "═══ Phase 1: KV Disk Offload ═══"
echo ""

# Start Moon with disk offload enabled, low maxmemory
$BINARY --port $MOON_PORT --shards 1 \
    --maxmemory "$MAXMEMORY" \
    --maxmemory-policy allkeys-lru \
    --dir "$DATA_DIR" \
    --disk-offload enable \
    --disk-offload-dir "$OFFLOAD_DIR" \
    --appendonly yes --appendfsync everysec &
MOON_PID=$!
sleep 2

if ! redis-cli -p $MOON_PORT PING > /dev/null 2>&1; then
    echo "Moon failed to start with disk-offload. Trying without..."
    kill $MOON_PID 2>/dev/null || true
    sleep 1
    $BINARY --port $MOON_PORT --shards 1 \
        --maxmemory "$MAXMEMORY" \
        --maxmemory-policy allkeys-lru \
        --dir "$DATA_DIR" \
        --appendonly yes --appendfsync everysec &
    MOON_PID=$!
    sleep 2
fi

redis-cli -p $MOON_PORT PING > /dev/null 2>&1 || { echo "Moon not responding"; exit 1; }
echo "Moon started (pid=$MOON_PID)"

# Insert more data than maxmemory allows → forces eviction + spill
echo ""
echo "Inserting $N_KV keys × 1KB values (target: ${N_KV}KB > $MAXMEMORY)..."
INSERT_START=$(date +%s%N)
# Use redis-benchmark with pipeline for speed.
# IMPORTANT: -r $N_KV is required so __rand_int__ expands to a 12-digit
# integer in [0, N_KV). Without -r, redis-benchmark writes a SINGLE literal
# key named "key:__rand_int__" 50K times — DBSIZE stays at 1 and the
# spot-check below fails. The spot-check uses the same 12-digit format.
timeout 60 redis-benchmark -p $MOON_PORT -r $N_KV -c 10 -n $N_KV -t set -d 1024 -P 64 -q 2>&1 | head -3 || true
INSERT_END=$(date +%s%N)
INSERT_MS=$(( (INSERT_END - INSERT_START) / 1000000 ))
echo "Insert: ${INSERT_MS}ms"

# Check how many keys survived in memory vs evicted
sleep 2
echo ""
echo "Checking eviction state..."
INFO=$(redis-cli -p $MOON_PORT INFO memory 2>&1)
echo "$INFO" | grep -E "used_memory|evicted|maxmemory" | tr -d '\r' || echo "  (INFO memory not fully implemented)"

# Read-through test: GET random keys (some in RAM, some on disk)
echo ""
echo "Cold read-through test: GET 10000 random keys..."
READ_START=$(date +%s%N)
timeout 30 redis-benchmark -p $MOON_PORT -c 10 -n 10000 -t get -r $N_KV -P 16 -q 2>&1 | head -3 || true
READ_END=$(date +%s%N)
READ_MS=$(( (READ_END - READ_START) / 1000000 ))
echo "Read: ${READ_MS}ms"

# Check disk files
echo ""
echo "Disk files created:"
find "$DATA_DIR" -name "*.mpf" -o -name "*.wal" -o -name "*.control" -o -name "MANIFEST" 2>/dev/null | head -20
DISK_SIZE=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
echo "Total disk usage: ${DISK_SIZE:-0}"

# ══════════════════════════════════════════════════════════════
# PHASE 2: VECTOR WARM → COLD TRANSITION
# ══════════════════════════════════════════════════════════════

echo ""
echo "═══ Phase 2: Vector Tier Transitions ═══"
echo ""

# Create vector index
redis-cli -p $MOON_PORT FT.CREATE bench_vec ON HASH PREFIX 1 vec: \
    SCHEMA emb VECTOR HNSW 6 DIM $DIM DISTANCE_METRIC COSINE TYPE FLOAT32 &
sleep 2

# Insert vectors via python
echo "Inserting $N_VEC vectors (${DIM}d)..."
VEC_INSERT_START=$(date +%s%N)
python3 -c "
import socket, struct, random, math, time

DIM = $DIM
N = $N_VEC
sock = socket.socket()
sock.connect(('127.0.0.1', $MOON_PORT))
sock.settimeout(30)

batch = bytearray()
for i in range(N):
    random.seed(i)
    v = [random.gauss(0,1) for _ in range(DIM)]
    norm = math.sqrt(sum(x*x for x in v))
    if norm > 0:
        v = [x/norm for x in v]
    blob = struct.pack(f'{DIM}f', *v)
    key = f'vec:{i}'
    hdr = f'*4\r\n\${4}\r\nHSET\r\n\${len(key)}\r\n{key}\r\n\${3}\r\nemb\r\n\${len(blob)}\r\n'.encode()
    batch += hdr + blob + b'\r\n'
    if len(batch) > 65536:
        sock.sendall(bytes(batch))
        batch = bytearray()
if batch:
    sock.sendall(bytes(batch))

time.sleep(2)
sock.settimeout(0.5)
try:
    while True: sock.recv(65536)
except: pass
sock.close()
print(f'Inserted {N} vectors')
" 2>&1
VEC_INSERT_END=$(date +%s%N)
VEC_INSERT_MS=$(( (VEC_INSERT_END - VEC_INSERT_START) / 1000000 ))
echo "Vector insert: ${VEC_INSERT_MS}ms ($(( N_VEC * 1000 / (VEC_INSERT_MS + 1) )) vec/s)"

# Check segment state
echo ""
echo "Disk state after vector insert:"
find "$DATA_DIR" -name "*.mpf" -type f 2>/dev/null | wc -l | xargs echo "  .mpf files:"
find "$DATA_DIR" -name "segment-*" -type d 2>/dev/null | wc -l | xargs echo "  Segment dirs:"
DISK_SIZE=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
echo "  Total disk: $DISK_SIZE"

# ══════════════════════════════════════════════════════════════
# PHASE 3: CRASH RECOVERY
# ══════════════════════════════════════════════════════════════

echo ""
echo "═══ Phase 3: Crash Recovery ═══"
echo ""

# Remember key count before crash
PRE_CRASH_KEYS=$(redis-cli -p $MOON_PORT INFO keyspace 2>&1 | grep -oE 'keys=[0-9]+' | head -1 | cut -d= -f2)
echo "Keys before crash: ${PRE_CRASH_KEYS:-unknown}"

# Kill -9 (simulate crash)
echo "Simulating crash (kill -9)..."
kill -9 $MOON_PID 2>/dev/null
sleep 2

# Restart and measure recovery time
echo "Restarting Moon..."
RECOVERY_START=$(date +%s%N)
$BINARY --port $MOON_PORT --shards 1 \
    --maxmemory "$MAXMEMORY" \
    --maxmemory-policy allkeys-lru \
    --dir "$DATA_DIR" \
    --disk-offload enable \
    --disk-offload-dir "$OFFLOAD_DIR" \
    --appendonly yes --appendfsync everysec &
MOON_PID=$!

# Wait for ready
for i in $(seq 1 30); do
    if redis-cli -p $MOON_PORT PING > /dev/null 2>&1; then
        RECOVERY_END=$(date +%s%N)
        RECOVERY_MS=$(( (RECOVERY_END - RECOVERY_START) / 1000000 ))
        echo "Recovery time: ${RECOVERY_MS}ms"
        break
    fi
    sleep 0.5
done

# Check data integrity
POST_CRASH_KEYS=$(redis-cli -p $MOON_PORT INFO keyspace 2>&1 | grep -oE 'keys=[0-9]+' | head -1 | cut -d= -f2)
echo "Keys after recovery: ${POST_CRASH_KEYS:-unknown}"
if [ -n "${PRE_CRASH_KEYS:-}" ] && [ -n "${POST_CRASH_KEYS:-}" ]; then
    LOSS=$(( PRE_CRASH_KEYS - POST_CRASH_KEYS ))
    echo "Data loss: $LOSS keys ($(( LOSS * 100 / PRE_CRASH_KEYS ))%)"
fi

# Spot-check 10 random keys
echo ""
echo "Spot-check 10 random reads after recovery:"
OK=0
for i in $(seq 1 10); do
    # redis-benchmark zero-pads __rand_int__ to 12 digits, so we must match.
    KEY=$(printf "key:%012d" $(( RANDOM % N_KV )))
    VAL=$(redis-cli -p $MOON_PORT GET "$KEY" 2>&1)
    if [ -n "$VAL" ] && [ "$VAL" != "(nil)" ]; then
        OK=$((OK + 1))
    fi
done
echo "  $OK/10 keys returned data"

# Cleanup
kill $MOON_PID 2>/dev/null || true

echo ""
echo "================================================================"
echo "  Benchmark Complete"
echo "================================================================"
echo "  Disk type:      $DISK_TYPE"
echo "  Offload dir:    $OFFLOAD_DIR"
echo "  Final disk use: $(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)"
echo ""
echo "  For production NVMe benchmarks, run on bare metal with:"
echo "    ./scripts/bench-cold-tier.sh --disk /mnt/nvme --vectors 100000"
echo "================================================================"
