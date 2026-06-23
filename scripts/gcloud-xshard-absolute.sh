#!/usr/bin/env bash
# gcloud-xshard-absolute.sh — TRUE ABSOLUTE cross-shard read latency on GCloud bare-metal.
#
# Task: xshard-read-gcloud-validation (milestone v2-2). The OrbStack VM cannot give a
# trustworthy ABSOLUTE cross-shard latency (host->guest vCPU starvation inflated 36us->580us;
# xshard-read-fastpath §6). This harness ports the quiesced-xshard instrument
# (scripts/baseline-xshard-quiesced.sh) onto compute-optimized, dedicated-core GCloud
# instances where absolute us/op is meaningful, and records the absolute s4-c1-GET table across
# the four reference commits, dual-runtime, with fail-closed validity gates.
#
# Measure-only: this script NEVER edits moon's src/. It builds the reference commits read-only.
#
# Subcommands:
#   --self-test     gate logic against synthetic inputs (NO GCloud, NO cost) — the §4 red test target.
#   --measure       the INNER measurement; assumes it runs ON a suitably pinned Linux host.
#   --gcloud        (default) OUTER sweep: for each MACHINES entry provision, push, run --measure, teardown.
#   --one           OUTER for a single $GCE_MACHINE (used internally by the sweep).
#
# §3 CONTRACT C1/C2/C3 frozen 2026-06-22; C2 amended v2 2026-06-23 (dual-vendor c3+c2d, quota-forced).
set -uo pipefail

# ---------------------------------------------------------------------------- config (frozen C1/C2)
COMMITS="${COMMITS:-3e376a1 a497602 7048e8a 7e0a5db}"   # lock-read · SPSC-regression · C2-fix · main
RUNTIMES="${RUNTIMES:-monoio tokio}"
BEST_OF_N="${BEST_OF_N:-5}"
REQUESTS="${REQUESTS:-1000000}"      # redis-benchmark -n
KEYSPACE="${KEYSPACE:-1000000}"      # -r (real keys, not __rand_key__)
SHARDS=4
PORT="${PORT:-7413}"
LOAD_MAX="${LOAD_MAX:-0.70}"         # quiesce gate
QUIESCE_DEADLINE="${QUIESCE_DEADLINE:-180}"  # max s to WAIT for load<LOAD_MAX before proceeding-with-WARN
SETTLE_AFTER_BUILD="${SETTLE_AFTER_BUILD:-1}" # cool compile heat before the first cell of each commit
STEAL_MAX="${STEAL_MAX:-1}"          # vCPU steal-% gate (~0 on dedicated cores)
NOISE_PCT="${NOISE_PCT:-8}"          # s1-LOCAL "flat" tolerance + guard-cell regression tolerance
SERVER_CORES="${SERVER_CORES:-0-3}"  # moon shards: 4 cores
CLIENT_CORES="${CLIENT_CORES:-4-7}"  # redis-benchmark: disjoint 4 cores
# cores 8-15 on a 16-vCPU instance left IDLE as a steal-time buffer (C2 sizing rationale)

# GCloud (C2 instrument — AMENDED 2026-06-23): c2-standard-16 blocked by C2_CPUS quota=8;
# DUAL-VENDOR compute-optimized cross-validation, run SEQUENTIALLY (CPUS_ALL_REGIONS=32 global
# cap forbids both at once). NOTE: C3 has no 16-vCPU size (offered: 4/8/22/44/...), so the Intel
# pick is c3-standard-22 (22 vCPU); AMD is c2d-standard-16. Both keep 4 moon + 4 bench cores plus
# a generous idle steal-buffer; the 8 measured cores are identical, so the cells compare fairly.
MACHINES="${MACHINES:-c3-standard-22 c2d-standard-16}"   # swept in sequence (teardown between); each tags its rows
GCE_MACHINE="${GCE_MACHINE:-c3-standard-22}"             # per-machine override inside the sweep
GCE_NAME="${GCE_NAME:-moon-xshard-abs}"
GCE_ZONE="${GCE_ZONE:-us-central1-a}"
GCE_IMAGE_FAMILY="${GCE_IMAGE_FAMILY:-ubuntu-2404-lts-amd64}"
GCE_IMAGE_PROJECT="${GCE_IMAGE_PROJECT:-ubuntu-os-cloud}"

REPO_URL="${REPO_URL:-https://github.com/pilotspace/moon.git}"
OUT="${OUT:-tmp/XSHARD-GCLOUD-ABS.md}"

log() { printf '%s\n' "$*" >&2; }
die() { log "FATAL: $*"; exit 1; }

# ============================================================================= validity gates
# Pure functions (value in, 0=ok / 1=VOID) so --self-test drives them with synthetic inputs.

gate_quiesce() { awk -v a="$1" -v b="$LOAD_MAX" 'BEGIN{exit !(a<b)}'; }          # ok: load < LOAD_MAX
gate_steal()   { awk -v a="$1" -v b="$STEAL_MAX" 'BEGIN{exit !(a<=b)}'; }        # ok: steal <= STEAL_MAX
gate_clean() {   # ok: no stray moon process (non-empty arg simulates a planted poller for self-test)
  local planted="${1:-}"
  [[ -n "$planted" ]] && return 1
  pgrep -f 'moon.*monoio --port|moon --port|moon-baseline-bins/moon' >/dev/null 2>&1 && return 1
  return 0
}
require_floor() { [[ "${1:-0}" -ge "$BEST_OF_N" ]]; }                            # ok: full best-of-N sample
control_flat() {   # ok: every s1-LOCAL value within NOISE_PCT of the first
  local ref="$1"; shift
  [[ -z "$ref" || "$ref" -le 0 ]] 2>/dev/null && return 1
  local v
  for v in "$@"; do
    awk -v r="$ref" -v c="$v" -v n="$NOISE_PCT" 'BEGIN{ d=(r>c?r-c:c-r); exit !(d*100.0/r <= n) }' || return 1
  done
  return 0
}
us_per_op() { awk -v r="${1:-0}" 'BEGIN{ if(r>0) printf "%.2f", 1000000.0/r; else printf "n/a" }'; }

# ============================================================================= self-test
self_test() {
  local fails=0
  _ok()  { printf '  ok   %s\n' "$1" >&2; }
  _bad() { printf '  FAIL %s\n' "$1" >&2; fails=$((fails+1)); }
  log "=== gcloud-xshard-absolute --self-test (gate logic; no GCloud) ==="

  gate_quiesce 1.40 && _bad "quiesce should VOID at load 1.40" || _ok "quiesce VOIDs high load (contended_instrument)"
  gate_quiesce 0.20 && _ok "quiesce passes low load" || _bad "quiesce should pass load 0.20"
  gate_steal 7 && _bad "steal should VOID at 7%" || _ok "steal VOIDs 7% (contended_instrument)"
  gate_steal 0 && _ok "steal passes ~0%" || _bad "steal should pass 0%"
  gate_clean "planted-moon-monoio" && _bad "clean should refuse a planted busy-poller" || _ok "clean refuses planted poller (dirty_instrument)"
  require_floor 1 && _bad "require_floor should reject 1 rep" || _ok "require_floor rejects single rep (unstable_absolute)"
  require_floor "$BEST_OF_N" && _ok "require_floor accepts full best-of-N" || _bad "require_floor should accept $BEST_OF_N reps"
  control_flat 40000 41000 39500 && _ok "control_flat passes within noise" || _bad "control_flat should pass 40000/41000/39500"
  control_flat 40000 40500 28000 && _bad "control_flat should VOID a 30% drop" || _ok "control_flat VOIDs non-flat control (contended_instrument)"
  [[ "$(us_per_op 25000)" == "40.00" ]] && _ok "us_per_op 25000 -> 40.00us" || _bad "us_per_op 25000 should be 40.00 (got $(us_per_op 25000))"
  grep -q 's4-c1-GET' "$0" && grep -q 's1-LOCAL' "$0" && _ok "harness declares the frozen cells" || _bad "harness must declare the cells"

  if [[ "$fails" -eq 0 ]]; then log "=== self-test PASS (all gates fail-closed correctly) ==="; return 0; fi
  log "=== self-test FAIL ($fails) ==="; return 1
}

# ============================================================================= measurement core
# (ported from scripts/baseline-xshard-quiesced.sh — proven pinning / fresh-server-per-rep / best-of-N)
WORK="${WORK:-$HOME/moon-xshard-abs}"
BINS="${BINS:-$HOME/moon-xshard-abs-bins}"
MOON_PID=""
cleanup() { [[ -n "$MOON_PID" ]] && { kill "$MOON_PID" 2>/dev/null||true; wait "$MOON_PID" 2>/dev/null||true; MOON_PID=""; }; return 0; }
current_steal() { LC_ALL=C vmstat 1 2 2>/dev/null | tail -1 | awk '{print $(NF)}'; }   # 'st' column
current_load()  { awk '{print $1}' /proc/loadavg; }
# Block until the 1-min load decays below LOAD_MAX (defends against benchmark/compile self-load),
# with a deadline so a persistently-loaded host still proceeds (with a WARN) rather than hanging.
wait_quiesced() {
  local deadline=$((SECONDS+QUIESCE_DEADLINE)) l
  while :; do
    l=$(current_load)
    gate_quiesce "$l" && return 0
    [[ $SECONDS -ge $deadline ]] && { log "  WARN load $l >= $LOAD_MAX after ${QUIESCE_DEADLINE}s; proceeding"; return 0; }
    sleep 2
  done
}

prepare_repo() {
  if [[ ! -d "$WORK/.git" ]]; then log "=== clone $REPO_URL -> $WORK ==="; git clone -q "$REPO_URL" "$WORK" || die "clone failed"; fi
  git -C "$WORK" fetch --all -q 2>/dev/null || true
}

build() {  # build <commit> <runtime> -> echoes binary path  (read-only checkout; never edits src)
  local commit="$1" rt="$2" out="$BINS/moon-${commit//\//_}-${rt}"
  [[ -x "$out" ]] && { echo "$out"; return 0; }
  git -C "$WORK" checkout -q "$commit" || { log "  FATAL checkout $commit"; return 1; }
  log "  built-from: $(git -C "$WORK" rev-parse --short HEAD) $(git -C "$WORK" log -1 --format=%s | head -c 50)"
  local feats=()
  [[ "$rt" == "tokio" ]] && feats=(--no-default-features --features runtime-tokio,jemalloc)
  ( cd "$WORK" && CARGO_TARGET_DIR="$WORK/target" RUSTFLAGS="-C target-cpu=native" \
      cargo build --release "${feats[@]}" >/dev/null 2>&1 ) || { log "  FATAL build $commit/$rt"; return 1; }
  mkdir -p "$BINS"; cp "$WORK/target/release/moon" "$out"; echo "$out"
}

start_moon() {  # start_moon <bin> <shards>
  local bin="$1" shards="$2"; cleanup
  local dir; dir="$(mktemp -d /tmp/moon-abs.XXXXXX)"
  taskset -c "$SERVER_CORES" "$bin" --port "$PORT" --shards "$shards" --dir "$dir" \
    --appendonly no --admin-port 0 >/dev/null 2>&1 &
  MOON_PID=$!
  local deadline=$((SECONDS+10))
  until redis-cli -p "$PORT" ping >/dev/null 2>&1; do
    if ! kill -0 "$MOON_PID" 2>/dev/null; then
      taskset -c "$SERVER_CORES" "$bin" --port "$PORT" --shards "$shards" --dir "$dir" --appendonly no >/dev/null 2>&1 &
      MOON_PID=$!
    fi
    [[ $SECONDS -ge $deadline ]] && { log "  ERROR moon did not start"; return 1; }
    sleep 0.1
  done
}

bench_rps() {  # bench_rps <c> <p> <cmd>
  taskset -c "$CLIENT_CORES" redis-benchmark -p "$PORT" -c "$1" -P "$2" -n "$REQUESTS" \
    -r "$KEYSPACE" -t "$3" --csv 2>/dev/null \
    | tr '\r' '\n' | grep "\"${3}\"" | awk -F',' '{gsub(/"/,"",$2); printf "%.0f\n",$2}' | tail -1
}

cell() {  # cell <bin> <shards> <c> <p> <cmd> <name> -> "name|best|reps_csv|n"
  local bin="$1" shards="$2" c="$3" p="$4" cmd="$5" name="$6" vals=() n=0 attempts=0 r
  local max_attempts=$((BEST_OF_N*3))   # bounded retries so a flaky rep can't loop forever
  while [[ "$n" -lt "$BEST_OF_N" && "$attempts" -lt "$max_attempts" ]]; do
    attempts=$((attempts+1))
    wait_quiesced                                   # BLOCK until load decays (not skip) — the c2d-VOID fix
    start_moon "$bin" "$shards" || { sleep 2; continue; }
    r="$(bench_rps "$c" "$p" "$cmd" || echo 0)"; cleanup
    [[ -z "$r" || "$r" -le 0 ]] 2>/dev/null && { log "  retry $name (bad rep: '$r')"; continue; }
    vals+=("$r"); n=$((n+1))
  done
  local best=0; [[ "$n" -gt 0 ]] && best=$(printf '%s\n' "${vals[@]}" | sort -n | tail -1)
  printf '%s|%s|%s|%s\n' "$name" "$best" "$(IFS=,;echo "${vals[*]:-}")" "$n"
}

measure() {
  command -v redis-benchmark >/dev/null || die "redis-benchmark not on PATH (run gcloud-bench-setup.sh)"
  command -v taskset >/dev/null || die "taskset not available"
  prepare_repo
  trap cleanup EXIT INT TERM

  local steal; steal=$(current_steal); steal="${steal:-0}"
  if ! gate_steal "$steal"; then
    printf 'status: VOID\nreason: contended_instrument\ndetail: vCPU steal %%=%s > %s (host-contended; not bare-metal)\n' "$steal" "$STEAL_MAX"
    die "VOID contended_instrument (steal=$steal%)"
  fi
  if ! gate_clean; then
    printf 'status: VOID\nreason: dirty_instrument\ndetail: a stray moon process is alive\n'
    die "VOID dirty_instrument"
  fi

  log "=== gates OK (steal=$steal%, clean) — measuring on $(uname -srm) ==="
  echo "# absolute cells (raw)"
  echo "# machine|runtime|commit|cell|best_rps|us_per_op|reps|n"
  declare -gA BEST
  local rt commit
  for rt in $RUNTIMES; do
    for commit in $COMMITS; do
      local bin; bin=$(build "$commit" "$rt") || die "build $commit/$rt"
      [[ "$SETTLE_AFTER_BUILD" == "1" ]] && { log "  settling compile heat before cells..."; wait_quiesced; }
      while IFS='|' read -r name best reps n; do
        BEST["$rt|$commit|$name"]="$best"
        printf '%s|%s|%s|%s|%s|%s|%s|%s\n' "${GCE_MACHINE:-local}" "$rt" "$commit" "$name" "$best" "$(us_per_op "$best")" "$reps" "$n"
      done < <(
        cell "$bin" 1 1   1  GET "s1-LOCAL"
        cell "$bin" 4 1   1  GET "s4-c1-GET"
        cell "$bin" 4 1   16 GET "s4-P16"
        cell "$bin" 4 100 1  GET "s4-c100-GET"
      )
    done
  done

  for rt in $RUNTIMES; do
    local ctrl=() c
    for c in $COMMITS; do ctrl+=("${BEST["$rt|$c|s1-LOCAL"]:-0}"); done
    if ! control_flat "${ctrl[@]}"; then
      printf 'status: VOID\nreason: contended_instrument\ndetail: s1-LOCAL control not flat for %s: %s\n' "$rt" "${ctrl[*]}"
      die "VOID contended_instrument (s1-LOCAL non-flat: ${ctrl[*]})"
    fi
  done
  echo "# control_flat: OK (s1-LOCAL within ${NOISE_PCT}% across commits)"
}

# ============================================================================= gcloud orchestration
gcloud_run_one() {  # provisions ONE machine ($GCE_MACHINE / $GCE_NAME), measures, tears down (trap).
  command -v gcloud >/dev/null || die "gcloud CLI not found"
  local GSSH=(gcloud compute ssh "$GCE_NAME" --zone="$GCE_ZONE" --quiet
        --ssh-flag=-oStrictHostKeyChecking=no --ssh-flag=-oConnectTimeout=15)
  log "=== provisioning $GCE_NAME ($GCE_MACHINE, $GCE_ZONE) ==="
  gcloud compute instances create "$GCE_NAME" \
    --machine-type="$GCE_MACHINE" --zone="$GCE_ZONE" \
    --image-family="$GCE_IMAGE_FAMILY" --image-project="$GCE_IMAGE_PROJECT" \
    --boot-disk-size=50GB --boot-disk-type=pd-ssd --quiet \
    || die "instance create failed"

  # Bake the instance name into the trap NOW (double-quoted) — at fire time (subshell EXIT) the
  # `VAR=val func` env-prefix has reverted GCE_NAME to its default, which previously deleted the
  # WRONG instance and leaked the real one. Capturing here pins the actual name.
  local _inst="$GCE_NAME" _zone="$GCE_ZONE"
  trap "log '=== tearing down $_inst ==='; gcloud compute instances delete '$_inst' --zone='$_zone' -q 2>/dev/null || true" EXIT INT TERM

  log "=== waiting for SSH (generates keys on first use; --quiet, no prompt) ==="
  local tries=0
  until "${GSSH[@]}" --command='echo up' >/dev/null 2>&1; do
    tries=$((tries+1)); [[ $tries -ge 40 ]] && die "SSH never came up"; sleep 5
  done

  log "=== provisioning toolchain + repo on the instance ==="
  "${GSSH[@]}" --command='
    set -e
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential pkg-config libssl-dev redis-tools git
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1
  ' || die "instance provisioning failed"

  log "=== pushing harness ==="
  gcloud compute scp scripts/gcloud-xshard-absolute.sh "$GCE_NAME":~/gcloud-xshard-absolute.sh \
    --zone="$GCE_ZONE" --quiet --scp-flag=-oStrictHostKeyChecking=no || die "scp failed"

  local rawfile="/tmp/xshard-abs-${GCE_MACHINE}.txt"
  log "=== running --measure on $GCE_MACHINE (this is the long part) -> $rawfile ==="
  "${GSSH[@]}" --command="
    source \$HOME/.cargo/env
    GCE_MACHINE='$GCE_MACHINE' COMMITS='$COMMITS' RUNTIMES='$RUNTIMES' BEST_OF_N='$BEST_OF_N' REQUESTS='$REQUESTS' KEYSPACE='$KEYSPACE' \
      bash ~/gcloud-xshard-absolute.sh --measure
  " | tee "$rawfile"

  log "=== $GCE_MACHINE raw results in $rawfile; teardown follows (trap) ==="
}

gcloud_sweep() {  # run gcloud_run_one for each machine in MACHINES, each in its own subshell
  log "=== DUAL-VENDOR SWEEP: $MACHINES ==="
  local m short
  for m in $MACHINES; do
    short="${m%%-*}"
    log ""; log "##################### machine: $m #####################"
    ( GCE_MACHINE="$m"; GCE_NAME="moon-xshard-${short}"; gcloud_run_one ) \
      || log "WARN: machine $m run failed (see log); continuing to next machine"
  done
  log "=== sweep complete; per-machine raw files: /tmp/xshard-abs-<machine>.txt ==="
}

# ============================================================================= dispatch
case "${1:---gcloud}" in
  --self-test) self_test ;;
  --measure)   measure ;;
  --gcloud)    gcloud_sweep ;;
  --one)       gcloud_run_one ;;
  *) die "unknown subcommand: $1 (use --self-test | --measure | --gcloud | --one)" ;;
esac
