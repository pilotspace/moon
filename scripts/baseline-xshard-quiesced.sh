#!/usr/bin/env bash
# baseline-xshard-quiesced.sh — quiesced-VM, core-pinned, best-of-N cross-shard read baseline.
#
# RELATIVE anchor for xshard-read-fastpath's "halve the gap" target. The OrbStack VM
# cannot give a trustworthy ABSOLUTE cross-shard latency (host->guest vCPU starvation,
# milestone exception 2026-06-13), but the SAME instrument used before AND after a change
# yields a valid RATIO: contention only ADDS latency, so best-of-N RPS is the latency
# floor, and any fixed VM/virtiofs overhead cancels in the pre/post ratio.
#
# Compares pre-regression (eb5d664, had the cross-thread RwLock read fast-path) vs current
# main (post-shardslice, SPSC round-trip) on the regression cell + neighbour + write control.
# Builds into a VM-LOCAL clone (no diskfull on /Volumes/Games; no virtiofs build slowdown).
#
# Usage (inside moon-dev):
#   bash scripts/baseline-xshard-quiesced.sh                 # monoio, both commits
#   RUNTIMES="monoio tokio" bash scripts/baseline-xshard-quiesced.sh
set -euo pipefail

SRC="${SRC:-/Volumes/Games/tindang-repo/moon}"   # clone source = the live local repo
WORK="${WORK:-$HOME/moon-baseline}"              # VM-local clone + target (VM disk, not /Volumes)
BINS="${BINS:-$HOME/moon-baseline-bins}"
REPS="${REPS:-5}"
REQUESTS="${REQUESTS:-200000}"
SHARDS=4
PORT=7412
KEYSPACE=1000000
LOAD_MAX="${LOAD_MAX:-0.70}"                      # only count a rep taken below this 1-min load
QUIESCE_DEADLINE="${QUIESCE_DEADLINE:-120}"       # max seconds wait_quiesced blocks before proceeding-with-WARN
SLEEP_BETWEEN="${SLEEP_BETWEEN:-0}"               # seconds to sleep AFTER each rep (lets self-generated load decay before re-gating)
SETTLE_MAX="${SETTLE_MAX:-0}"                     # if >0: deep-settle before each commit until 1-min load < this (or SETTLE_TIMEOUT)
SETTLE_TIMEOUT="${SETTLE_TIMEOUT:-300}"           # max seconds to wait for deep-settle
RUNTIMES="${RUNTIMES:-monoio}"                    # space-sep: monoio tokio
BASELINE_COMMIT="${BASELINE_COMMIT:-3e376a1}"     # v0.3.0 tag = pre-shared-nothing main, has the lock read fast-path (~190k c1 GET per shardslice §6)
CURRENT_COMMIT="${CURRENT_COMMIT:-a497602}"       # current main HEAD (shared-nothing, SPSC round-trip); explicit hash avoids detached-HEAD ambiguity in the clone

SERVER_CORES="${SERVER_CORES:-0-3}"               # 4 shards, one per core
CLIENT_CORES="${CLIENT_CORES:-4,5}"               # redis-benchmark isolated from server cores

MOON_PID=""
cleanup() { [[ -n "$MOON_PID" ]] && { kill "$MOON_PID" 2>/dev/null||true; wait "$MOON_PID" 2>/dev/null||true; MOON_PID=""; }; return 0; }
trap cleanup EXIT INT TERM

log() { printf '%s\n' "$*" >&2; }

# Block until the VM 1-min load is quiesced (defends against host vCPU starvation episodes).
wait_quiesced() {
  local deadline=$((SECONDS+QUIESCE_DEADLINE))
  while :; do
    local l; l=$(awk '{print $1}' /proc/loadavg)
    awk -v a="$l" -v b="$LOAD_MAX" 'BEGIN{exit !(a<b)}' && return 0
    [[ $SECONDS -ge $deadline ]] && { log "  WARN: load $l still >= $LOAD_MAX after ${QUIESCE_DEADLINE}s; proceeding"; return 0; }
    sleep 2
  done
}

# Deep-settle: builds + the previous commit's c100 cell leave the 1-min loadavg elevated
# (it LAGS ~1min). Before measuring a commit, wait for the average to genuinely decay so the
# c1 latency cells start from a cool VM. No-op unless SETTLE_MAX>0.
deep_settle() {
  awk -v m="$SETTLE_MAX" 'BEGIN{exit !(m>0)}' || return 0
  local deadline=$((SECONDS+SETTLE_TIMEOUT)) l
  log "  deep-settle: waiting for 1-min load < $SETTLE_MAX (timeout ${SETTLE_TIMEOUT}s)..."
  while :; do
    l=$(awk '{print $1}' /proc/loadavg)
    awk -v a="$l" -v b="$SETTLE_MAX" 'BEGIN{exit !(a<b)}' && { log "  deep-settle: load $l < $SETTLE_MAX — measuring"; return 0; }
    [[ $SECONDS -ge $deadline ]] && { log "  deep-settle: load $l still >= $SETTLE_MAX after ${SETTLE_TIMEOUT}s; measuring anyway"; return 0; }
    sleep 3
  done
}

prepare_repo() {
  if [[ ! -d "$WORK/.git" ]]; then
    log "=== cloning $SRC -> $WORK (VM-local) ==="
    git clone "file://$SRC" "$WORK"
  fi
  git -C "$WORK" fetch --all -q 2>/dev/null || true
}

# build <commit> <runtime> -> echoes binary path
build() {
  local commit="$1" rt="$2"
  local out="$BINS/moon-${commit//\//_}-${rt}"
  [[ -x "$out" ]] && { echo "$out"; return 0; }
  log "=== build commit=$commit runtime=$rt ==="
  # HARD GUARD: never silently build the wrong tree (the relabel bug). Abort on bad checkout.
  if ! git -C "$WORK" checkout -q "$commit"; then
    log "  FATAL: checkout '$commit' failed — refusing to build a mislabeled binary"; return 1
  fi
  log "  checked out: $(git -C "$WORK" rev-parse --short HEAD) $(git -C "$WORK" log -1 --format=%s | head -c 60)"
  local feats=()
  if [[ "$rt" == "tokio" ]]; then
    feats=(--no-default-features --features runtime-tokio,jemalloc)
  fi
  ( cd "$WORK" && CARGO_TARGET_DIR="$WORK/target" RUSTFLAGS="-C target-cpu=native" \
      cargo build --release "${feats[@]}" >/dev/null 2>&1 )
  mkdir -p "$BINS"
  cp "$WORK/target/release/moon" "$out"
  echo "$out"
}

# Robust start: minimal flags valid across both commits; fresh VM-local --dir.
start_moon() {
  local bin="$1"
  cleanup
  local dir; dir="$(mktemp -d /tmp/moon-base.XXXXXX)"
  taskset -c "$SERVER_CORES" "$bin" --port "$PORT" --shards "$SHARDS" --dir "$dir" \
    --appendonly no --admin-port 0 >/dev/null 2>&1 &
  MOON_PID=$!
  local deadline=$((SECONDS+10))
  until redis-cli -p "$PORT" ping >/dev/null 2>&1; do
    if ! kill -0 "$MOON_PID" 2>/dev/null; then
      # flag mismatch on an older commit — retry without --admin-port
      taskset -c "$SERVER_CORES" "$bin" --port "$PORT" --shards "$SHARDS" --dir "$dir" \
        --appendonly no >/dev/null 2>&1 &
      MOON_PID=$!
    fi
    [[ $SECONDS -ge $deadline ]] && { log "  ERROR: moon ($bin) did not start"; return 1; }
    sleep 0.1
  done
}

bench_rps() {
  local c="$1" p="$2" cmd="$3"
  taskset -c "$CLIENT_CORES" redis-benchmark -p "$PORT" -c "$c" -P "$p" -n "$REQUESTS" \
    -r "$KEYSPACE" -t "$cmd" --csv 2>/dev/null \
    | tr '\r' '\n' | grep "\"${cmd}\"" | awk -F',' '{gsub(/"/,"",$2); printf "%.0f\n",$2}' | tail -1
}

# cell <bin> <c> <p> <cmd> <name>  -> "name|reps|best|median"
cell() {
  local bin="$1" c="$2" p="$3" cmd="$4" name="$5"
  local vals=()
  for ((i=1;i<=REPS;i++)); do
    wait_quiesced
    start_moon "$bin" || { vals+=(0); continue; }
    vals+=("$(bench_rps "$c" "$p" "$cmd" || echo 0)")
    cleanup
    [[ "$SLEEP_BETWEEN" -gt 0 ]] && sleep "$SLEEP_BETWEEN"
  done
  local best med
  best=$(printf '%s\n' "${vals[@]}" | sort -n | tail -1)
  med=$(printf '%s\n' "${vals[@]}" | sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}')
  printf '%s|%s|%s|%s\n' "$name" "$(IFS=,;echo "${vals[*]}")" "$best" "$med"
}

prepare_repo
echo "###### QUIESCED-VM BASELINE  shards=$SHARDS reqs=$REQUESTS reps=$REPS load_max=$LOAD_MAX"
echo "###### server_cores=$SERVER_CORES client_cores=$CLIENT_CORES  host=$(uname -srm)"
echo "###### NOTE: best-of-N RPS = latency floor; anchor is the RELATIVE ratio (overhead cancels)."

declare -A RESULT
for rt in $RUNTIMES; do
  for commit in "$BASELINE_COMMIT" "$CURRENT_COMMIT"; do
    if ! bin=$(build "$commit" "$rt"); then
      log "FATAL: build failed for commit=$commit runtime=$rt — aborting baseline"; exit 1
    fi
    log "--- measuring commit=$commit runtime=$rt bin=$bin ---"
    deep_settle   # cool the VM after the build / previous commit's c100 before the latency-sensitive c1 cells
    # ORDER MATTERS: c1 latency cells run while the VM is cool; c100 (which heats the VM) runs LAST so it
    # never contaminates a following c1 cell. deep_settle re-cools before the next commit's c1.
    while IFS='|' read -r name reps best med; do
      RESULT["$rt|$commit|$name"]="$reps|$best|$med"
      printf '%-8s %-9s %-26s reps=[%s] best=%s med=%s RPS\n' "$rt" "$commit" "$name" "$reps" "$best" "$med"
    done < <(
      cell "$bin" 1   1 GET "s4-c1-GET"
      cell "$bin" 1   1 SET "s4-c1-SET"
      cell "$bin" 100 1 GET "s4-c100-GET"
    )
  done
done

echo
echo "###### RELATIVE ANCHOR (best-of-N RPS; regression = (pre - cur)/pre on RPS) ######"
for rt in $RUNTIMES; do
  for name in s4-c1-GET s4-c100-GET s4-c1-SET; do
    pre="${RESULT["$rt|$BASELINE_COMMIT|$name"]:-}"; cur="${RESULT["$rt|$CURRENT_COMMIT|$name"]:-}"
    [[ -z "$pre" || -z "$cur" ]] && continue
    pb=$(echo "$pre" | cut -d'|' -f2); cb=$(echo "$cur" | cut -d'|' -f2)
    reg=$(awk -v p="$pb" -v c="$cb" 'BEGIN{ if(p>0) printf "%+.1f", (p-c)*100.0/p; else print "n/a" }')
    printf '%-8s %-26s pre(%s)=%s  cur(%s)=%s  RPS-regression=%s%%\n' \
      "$rt" "$name" "$BASELINE_COMMIT" "$pb" "$CURRENT_COMMIT" "$cb" "$reg"
  done
done
echo "###### END BASELINE ######"
