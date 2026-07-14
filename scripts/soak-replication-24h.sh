#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# soak-replication-24h.sh -- replication kill-9 soak harness (task #61)
#
# Gates the Moon v0.7.0 "Replication GA for multi-shard masters" release
# headline. Runs a master + replica pair under continuous acked-write load
# (scripts/soak_replication_driver.py) while periodically kill -9'ing one
# side, restarting it, waiting for a DATA-DRIVEN resync (not just
# master_link_status:up), and asserting zero loss of every WAIT-acknowledged
# write on BOTH master and replica.
#
# Usage:
#   ./scripts/soak-replication-24h.sh                # full 24h soak
#   ./scripts/soak-replication-24h.sh --smoke         # 30-minute smoke run
#   ./scripts/soak-replication-24h.sh --duration 3600 --cycle-interval 600
#
# Must run on the moon-dev OrbStack VM (Linux), from a VM-LOCAL clone -- see
# CLAUDE.md OrbStack section. Never point --workdir at /Volumes/Games (near
# the 5% diskfull-guard line) or /tmp (4.7G tmpfs, fills fast over 24h).
#
# Exit code 0 = PASS (every acked write present + correct on both sides for
# the whole run). Exit code 1 = SOAK-FAIL (see stdout for the failing
# seq/side/cycle) -- this MUST block the v0.7.0 tag.
###############################################################################

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DURATION=86400            # 24h
CYCLE_INTERVAL=720        # 12 min, alternating master/replica kill -9
CYCLE_INTERVAL_SET=false
WORKDIR="${HOME}/moon-soak"
REPO_URL="file:///Volumes/Games/tindang-repo/moon"
FRESH_CLONE=false
SKIP_BUILD=false
MASTER_PORT=17400
REPLICA_PORT=17401
MASTER_SHARDS=4
# NOTE: moon's replica-side streaming replication currently supports
# --shards 1 ONLY (src/replication/replica.rs errors out otherwise: "Replica:
# streaming replication currently supports single-shard only"). Every
# scenario in tests/replication_multishard.rs spawns its replica with
# --shards 1 for the same reason -- R2 (task #20) made the MASTER side
# multi-shard-capable, the replica side was not part of that scope. Do not
# raise this until replica-side multi-shard streaming ships.
REPLICA_SHARDS=1
RATE=10                   # writer ops/sec
WAIT_TIMEOUT_MS=3000
CATCHUP_TIMEOUT_SEC=120
VERIFY_SAMPLE=1000
VERIFY_TAIL=200
SMOKE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke)            SMOKE=true; shift ;;
        --duration)          DURATION="$2"; shift 2 ;;
        --cycle-interval)    CYCLE_INTERVAL="$2"; CYCLE_INTERVAL_SET=true; shift 2 ;;
        --workdir)           WORKDIR="$2"; shift 2 ;;
        --repo-url)          REPO_URL="$2"; shift 2 ;;
        --fresh-clone)       FRESH_CLONE=true; shift ;;
        --skip-build)        SKIP_BUILD=true; shift ;;
        --master-port)       MASTER_PORT="$2"; shift 2 ;;
        --replica-port)      REPLICA_PORT="$2"; shift 2 ;;
        --master-shards)     MASTER_SHARDS="$2"; shift 2 ;;
        --replica-shards)    REPLICA_SHARDS="$2"; shift 2 ;;
        --rate)               RATE="$2"; shift 2 ;;
        --wait-timeout-ms)    WAIT_TIMEOUT_MS="$2"; shift 2 ;;
        --catchup-timeout)    CATCHUP_TIMEOUT_SEC="$2"; shift 2 ;;
        --verify-sample)      VERIFY_SAMPLE="$2"; shift 2 ;;
        --verify-tail)        VERIFY_TAIL="$2"; shift 2 ;;
        --help|-h)
            sed -n '3,26p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ "$SMOKE" == true ]]; then
    DURATION=1800
    [[ "$CYCLE_INTERVAL_SET" == false ]] && CYCLE_INTERVAL=480   # 3 cycles in 24 of the 30 min
fi

REPO_DIR="${WORKDIR}/repo"
RUN_DIR="${WORKDIR}/runs/$(date '+%Y%m%d-%H%M%S')"
MASTER_DIR="${RUN_DIR}/data/master"
REPLICA_DIR="${RUN_DIR}/data/replica"
LEDGER="${RUN_DIR}/ledger.txt"
INFLIGHT="${RUN_DIR}/inflight.txt"
DRIVER_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/soak_replication_driver.py"
DRIVER="${RUN_DIR}/soak_replication_driver.py"
SOAK_LOG="${RUN_DIR}/soak.log"

MASTER_PID=""
REPLICA_PID=""
WRITER_PID=""
FAILED=false

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$SOAK_LOG" >&2
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

mkdir -p "$WORKDIR" "$MASTER_DIR" "$REPLICA_DIR"
mkdir -p "$RUN_DIR"
touch "$SOAK_LOG"
: > "$LEDGER"
: > "$INFLIGHT"
cp "$DRIVER_SRC" "$DRIVER"

log "============================================================"
log "Moon replication soak (task #61) -- duration=${DURATION}s cycle-interval=${CYCLE_INTERVAL}s"
log "run dir: $RUN_DIR"
log "============================================================"

if [[ "$FRESH_CLONE" == true && -d "$REPO_DIR" ]]; then
    log "removing existing clone for --fresh-clone"
    rm -rf "$REPO_DIR"
fi

if [[ ! -d "${REPO_DIR}/.git" ]]; then
    log "cloning $REPO_URL -> $REPO_DIR"
    git clone --depth 1 "$REPO_URL" "$REPO_DIR" >>"$SOAK_LOG" 2>&1
fi

MOON_BIN="${REPO_DIR}/target/release/moon"

if [[ "$SKIP_BUILD" == false || ! -x "$MOON_BIN" ]]; then
    log "building moon (nice -n 10, jobs=4) in $REPO_DIR"
    (
        cd "$REPO_DIR"
        source "$HOME/.cargo/env"
        nice -n 10 cargo build --release --jobs 4
    ) >>"$SOAK_LOG" 2>&1
fi

if [[ ! -x "$MOON_BIN" ]]; then
    log "FATAL: $MOON_BIN not built"
    exit 1
fi

# Guard against the documented OrbStack trap: a stale macOS Mach-O binary
# silently exec'd via the host-proxy (logs "Listening" anyway).
ELF_MAGIC=$(od -An -tx1 -N4 "$MOON_BIN" | tr -s ' ')
if [[ "$ELF_MAGIC" != *"7f 45 4c 46"* ]]; then
    log "FATAL: $MOON_BIN is not an ELF binary (magic:${ELF_MAGIC}) -- stale Mach-O or corrupt build"
    exit 1
fi
log "moon binary verified ELF: $MOON_BIN"

for PORT in "$MASTER_PORT" "$REPLICA_PORT"; do
    if timeout 2 redis-cli -p "$PORT" PING >/dev/null 2>&1; then
        log "FATAL: something is already answering PING on port $PORT -- pick a different --master-port/--replica-port"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Process helpers -- PID-targeted kill -9 ONLY, never a broad pkill pattern
# while the soak is running (CLAUDE.md SO_REUSEPORT hang trap + repo rule).
# ---------------------------------------------------------------------------

wait_ready() {
    local port=$1 timeout_s=${2:-30}
    local deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) < deadline )); do
        if timeout 2 redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 0.3
    done
    return 1
}

wait_link_up() {
    local port=$1 timeout_s=${2:-30}
    local deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) < deadline )); do
        if timeout 2 redis-cli -p "$port" INFO replication 2>/dev/null | grep -q 'master_link_status:up'; then
            return 0
        fi
        sleep 0.3
    done
    return 1
}

start_master() {
    log "starting master on port $MASTER_PORT (shards=$MASTER_SHARDS, dir=$MASTER_DIR)"
    "$MOON_BIN" --port "$MASTER_PORT" --shards "$MASTER_SHARDS" --dir "$MASTER_DIR" \
        --appendonly yes --appendfsync always --disk-free-min-pct 0 \
        >>"${RUN_DIR}/master.log" 2>&1 &
    MASTER_PID=$!
    if ! wait_ready "$MASTER_PORT" 30; then
        log "FATAL: master did not become ready on port $MASTER_PORT"
        exit 1
    fi
    log "master PID=$MASTER_PID ready"
}

start_replica() {
    log "starting replica on port $REPLICA_PORT (shards=$REPLICA_SHARDS, dir=$REPLICA_DIR)"
    "$MOON_BIN" --port "$REPLICA_PORT" --shards "$REPLICA_SHARDS" --dir "$REPLICA_DIR" \
        --disk-free-min-pct 0 \
        >>"${RUN_DIR}/replica.log" 2>&1 &
    REPLICA_PID=$!
    if ! wait_ready "$REPLICA_PORT" 30; then
        log "FATAL: replica did not become ready on port $REPLICA_PORT"
        exit 1
    fi
    log "replica PID=$REPLICA_PID ready"
}

attach_replica() {
    if ! timeout 5 redis-cli -p "$REPLICA_PORT" REPLICAOF 127.0.0.1 "$MASTER_PORT" | grep -qi OK; then
        log "FATAL: REPLICAOF failed"
        exit 1
    fi
    if ! wait_link_up "$REPLICA_PORT" 30; then
        log "FATAL: replica link never came up after REPLICAOF"
        exit 1
    fi
    log "replica attached and link up"
}

kill_pid() {
    local pid=$1 label=$2
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        log "killed $label (PID $pid)"
    fi
}

cleanup() {
    log "cleanup: stopping writer + servers"
    if [[ -n "$WRITER_PID" ]] && kill -0 "$WRITER_PID" 2>/dev/null; then
        kill -TERM "$WRITER_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            kill -0 "$WRITER_PID" 2>/dev/null || break
            sleep 0.5
        done
        kill -9 "$WRITER_PID" 2>/dev/null || true
        wait "$WRITER_PID" 2>/dev/null || true
    fi
    kill_pid "$MASTER_PID" master
    kill_pid "$REPLICA_PID" replica
}
trap cleanup EXIT INT TERM

fail_and_exit() {
    FAILED=true
    log "SOAK-FAIL -- aborting soak (see message above)"
    exit 1
}

# ---------------------------------------------------------------------------
# Boot master + replica, attach, start the writer
# ---------------------------------------------------------------------------

start_master
start_replica
attach_replica

log "starting writer (rate=${RATE}/s wait-timeout-ms=${WAIT_TIMEOUT_MS})"
python3 "$DRIVER" writer \
    --master "127.0.0.1:${MASTER_PORT}" \
    --ledger "$LEDGER" \
    --inflight "$INFLIGHT" \
    --rate "$RATE" \
    --wait-timeout-ms "$WAIT_TIMEOUT_MS" \
    >>"${RUN_DIR}/writer.log" 2>&1 &
WRITER_PID=$!
sleep 3
if ! kill -0 "$WRITER_PID" 2>/dev/null; then
    log "FATAL: writer died immediately -- see ${RUN_DIR}/writer.log"
    tail -n 40 "${RUN_DIR}/writer.log" | tee -a "$SOAK_LOG" >&2
    exit 1
fi
log "writer PID=$WRITER_PID running"

# ---------------------------------------------------------------------------
# Chaos + verify cycle
# ---------------------------------------------------------------------------

CYCLE=0
MASTER_KILLS=0
REPLICA_KILLS=0

run_chaos_cycle() {
    local target=$1
    CYCLE=$((CYCLE + 1))
    log "cycle ${CYCLE}: kill -9 target=${target}"

    if [[ "$target" == "master" ]]; then
        kill_pid "$MASTER_PID" master
        MASTER_KILLS=$((MASTER_KILLS + 1))
        sleep 1
        start_master
        # Replica keeps its own REPLICAOF target and reconnects on its
        # internal exponential-backoff loop (500ms doubling to a 30s cap,
        # src/replication/replica.rs) -- no command needed here, just wait
        # generously for the link to flip back up (worst case the replica is
        # mid-backoff right when the master becomes ready again).
        if ! wait_link_up "$REPLICA_PORT" 60; then
            log "SOAK-FAIL cycle=${CYCLE} reason=replica-link-never-recovered-after-master-restart"
            fail_and_exit
        fi
    else
        kill_pid "$REPLICA_PID" replica
        REPLICA_KILLS=$((REPLICA_KILLS + 1))
        sleep 1
        start_replica
        # A killed replica loses its in-memory REPLICAOF state -- must be
        # re-issued after restart (tests/replication_hardening.rs pattern).
        attach_replica
    fi

    # Data-driven resync gate: link-up alone does not mean the backlog/RDB
    # replay has landed. Poll until the replica's last few acked writes
    # actually read back correctly before trusting the strict verify below.
    if ! python3 "$DRIVER" catchup \
        --replica "127.0.0.1:${REPLICA_PORT}" \
        --ledger "$LEDGER" \
        --timeout-sec "$CATCHUP_TIMEOUT_SEC" \
        --cycle "$CYCLE"; then
        log "SOAK-FAIL cycle=${CYCLE} reason=catchup-timeout"
        fail_and_exit
    fi

    if ! python3 "$DRIVER" verify \
        --master "127.0.0.1:${MASTER_PORT}" \
        --replica "127.0.0.1:${REPLICA_PORT}" \
        --ledger "$LEDGER" \
        --sample "$VERIFY_SAMPLE" \
        --tail "$VERIFY_TAIL" \
        --cycle "$CYCLE"; then
        fail_and_exit
    fi

    log "cycle ${CYCLE} (${target} kill/restart) verified OK"
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

START_TS=$(date +%s)
ELAPSED=0
NEXT_HOUR=3600
TARGETS=(master replica)
IDX=0

while (( ELAPSED < DURATION )); do
    REMAINING=$(( DURATION - ELAPSED ))
    (( REMAINING <= 0 )) && break
    SLEEP_FOR=$CYCLE_INTERVAL
    (( SLEEP_FOR > REMAINING )) && SLEEP_FOR=$REMAINING
    sleep "$SLEEP_FOR"
    ELAPSED=$(( $(date +%s) - START_TS ))

    while (( ELAPSED >= NEXT_HOUR )); do
        ACKED=$(wc -l < "$LEDGER" 2>/dev/null | tr -d ' ')
        HOUR=$(( NEXT_HOUR / 3600 ))
        log "SOAK-OK hour=${HOUR} acked=${ACKED:-0} cycles=${CYCLE} master_kills=${MASTER_KILLS} replica_kills=${REPLICA_KILLS}"
        NEXT_HOUR=$(( NEXT_HOUR + 3600 ))
    done

    (( ELAPSED >= DURATION )) && break

    TARGET="${TARGETS[$((IDX % 2))]}"
    IDX=$((IDX + 1))
    run_chaos_cycle "$TARGET"
done

# ---------------------------------------------------------------------------
# Final sweep + verdict
# ---------------------------------------------------------------------------

log "soak duration elapsed -- stopping writer for final full-ledger sweep"
if [[ -n "$WRITER_PID" ]] && kill -0 "$WRITER_PID" 2>/dev/null; then
    kill -TERM "$WRITER_PID" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$WRITER_PID" 2>/dev/null || break
        sleep 0.5
    done
    kill -9 "$WRITER_PID" 2>/dev/null || true
    wait "$WRITER_PID" 2>/dev/null || true
fi
WRITER_PID=""

if ! python3 "$DRIVER" verify \
    --master "127.0.0.1:${MASTER_PORT}" \
    --replica "127.0.0.1:${REPLICA_PORT}" \
    --ledger "$LEDGER" \
    --full \
    --cycle final; then
    fail_and_exit
fi

ACKED=$(wc -l < "$LEDGER" 2>/dev/null | tr -d ' ')
INFLIGHT_N=$(wc -l < "$INFLIGHT" 2>/dev/null | tr -d ' ')
log "============================================================"
log "SOAK-PASS duration=${DURATION}s cycles=${CYCLE} acked=${ACKED:-0} inflight=${INFLIGHT_N:-0} master_kills=${MASTER_KILLS} replica_kills=${REPLICA_KILLS}"
log "run dir: $RUN_DIR"
log "============================================================"
exit 0
