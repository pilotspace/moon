#!/usr/bin/env bash
# gcloud-kv-p1-baseline.sh — Moon vs Redis, shard=1, p=1 c1 single-op GET/SET latency,
# on GCloud DEDICATED-vCPU (steal-gated) instances, LOOPBACK, cross-arch (x86 + ARM).
#
# Goal (2026-07-03): ship the KV store at shard=1 that OUTPERFORMS Redis on p=1
# single-op latency, proven on GCloud dedicated/bare-metal, on both ARM and x86.
# This is the BASELINE instrument: does clean shipped Moon already beat Redis at p=1
# once shared-vCPU steal (the factor the KV deep review blamed) is removed?
#
# Topology = LOOPBACK (client + server co-located over lo). This is the ONLY topology
# where "our KV code beats Redis at p=1" is a code claim; cross-host p=1 measures TCP
# RTT (~50-150us), which both engines pay equally and which swamps the ~1.8us compute
# delta. Same topology as the OrbStack run where Moon won -> apples-to-apples, minus steal.
#
# Fair-config discipline (traps from prior GCloud runs, baked in):
#   * Both engines AOF-OFF (Moon --appendonly no ; Redis --appendonly no --save "").
#   * Canonical builds: Moon `cargo build --release` (shipped profile), Redis `make` (-O2).
#     No target-cpu=native (a bench-special flag, not shipped) -> generic vs generic, fair.
#   * ONE redis-benchmark (from the Redis source build) drives BOTH engines -> same client.
#   * Real keyspace (-r) not __rand_key__ ; SET-then-GET so GET is hit-path.
#   * Fresh-server-per-rep, best-of-N, quiesce-per-rep, dedicated-core steal gate.
#   * ELF-magic assert on the built moon binary (Mach-O trap).
#
# Measure-only: NEVER edits moon/src. Builds a clean committed ref read-only.
#
# Subcommands:
#   --self-test   gate + verdict logic against synthetic inputs (NO GCloud, NO cost).
#   --measure     INNER measurement; assumes it runs ON a suitably pinned Linux host.
#   --gcloud      (default) OUTER sweep: for each MACHINES entry provision/build/measure/teardown.
#   --one         OUTER for a single $GCE_MACHINE.
set -uo pipefail

# ---------------------------------------------------------------------------- config
MOON_REF="${MOON_REF:-main}"          # clean committed ref to baseline (NOT the dirty tree)
REDIS_VER="${REDIS_VER:-7.4.2}"
BEST_OF_N="${BEST_OF_N:-5}"
REQUESTS="${REQUESTS:-500000}"        # redis-benchmark -n ; ~5-10s/rep at c1p1
KEYSPACE="${KEYSPACE:-1000000}"       # -r (real keys)
MOON_PORT="${MOON_PORT:-7501}"
REDIS_PORT="${REDIS_PORT:-7502}"
LOAD_MAX="${LOAD_MAX:-0.70}"
QUIESCE_DEADLINE="${QUIESCE_DEADLINE:-180}"
STEAL_MAX="${STEAL_MAX:-1}"           # vCPU steal-% gate (~0 on dedicated cores)
NOISE_PCT="${NOISE_PCT:-8}"           # tie-band + control flatness tolerance
SERVER_CORE="${SERVER_CORE:-0}"       # single shard -> single core
CLIENT_CORE="${CLIENT_CORE:-2}"       # disjoint client core (cores 1,3 idle buffer)

# --- throughput mode (--measure-tput): rigorous pipeline-depth sweep, same pin/gate/best-of-N rigor as p=1 ---
# Motivation: unpinned single-run bench-compare cells are confounded (client/server core contention).
# This reuses the p=1 primitives (pinned disjoint cores, steal gate, fresh-server best-of-N) but sweeps
# pipeline depth so "faster per core at depth" is a CLEAN claim, not a scheduling artifact.
PIPES="${PIPES:-1}"                    # -P sweep (space-sep). p1 mode leaves this at 1 (unused).
CLIENTS_SWEEP="${CLIENTS_SWEEP:-1}"    # -c sweep. c=1 = cleanest per-core saturation; add 50 for concurrency.
BENCH_PIPE="${BENCH_PIPE:-1}"          # current -P for bench_pair (measure_tput sets per depth; p1 default 1)
BENCH_CLIENTS="${BENCH_CLIENTS:-1}"    # current -c for bench_pair (measure_tput sets per client; p1 default 1)
# PERSIST=memory (pure in-memory, both AOF-off — the fair speed/latency bar) | aof (both --appendonly yes
# --appendfsync everysec, per-engine temp --dir — the "durable" comparison). At shards=1, Moon AOF is AOF-only
# (per_shard_aof_active needs >=2) so NO WAL double-write. Redis AOF-on everysec = the matching config.
PERSIST="${PERSIST:-memory}"

# --- shard-scaling mode (--measure-shards): shards x busy-poll sweep on an 8-vCPU instance ---
SHARDS_SWEEP="${SHARDS_SWEEP:-1 4}"          # moon --shards cells
SPIN_SWEEP="${SPIN_SWEEP:-0 40}"             # --io-busy-poll-us cells (0 = off)
SHARD_CLIENT_CORES="${SHARD_CLIENT_CORES:-5-7}"  # client core range (server takes 0..shards-1)
BENCH_THREADS="${BENCH_THREADS:-}"           # client --threads for multi-conn cells (set per combo)

# machines: x86 FIRST (cheapest decisive case), ARM second (config flip)
MACHINES="${MACHINES:-c3-standard-4}"          # ARM run: MACHINES=c4a-standard-4 (or t2a-standard-4)
GCE_MACHINE="${GCE_MACHINE:-c3-standard-4}"
GCE_NAME="${GCE_NAME:-moon-kv-p1}"
GCE_ZONE="${GCE_ZONE:-us-central1-a}"
REPO_URL="${REPO_URL:-https://github.com/pilotspace/moon.git}"
OUT="${OUT:-tmp/KV-P1-BASELINE.md}"

log() { printf '%s\n' "$*" >&2; }
die() { log "FATAL: $*"; exit 1; }

# ============================================================================= pure gates + verdict
# Value-in / 0=ok so --self-test drives them with synthetic inputs (no GCloud).
gate_quiesce() { awk -v a="$1" -v b="$LOAD_MAX"  'BEGIN{exit !(a<b)}'; }        # ok: load < LOAD_MAX
gate_steal()   { awk -v a="$1" -v b="$STEAL_MAX" 'BEGIN{exit !(a<=b)}'; }       # ok: steal <= STEAL_MAX
gate_clean() {   # ok: no stray moon/redis-server (non-empty arg simulates a planted proc for self-test)
  local planted="${1:-}"
  [[ -n "$planted" ]] && return 1
  pgrep -f 'moon.*monoio --port|moon --port|moon-baseline-bins/moon' >/dev/null 2>&1 && return 1
  pgrep -x 'redis-server' >/dev/null 2>&1 && return 1
  return 0
}
require_floor() { [[ "${1:-0}" -ge "$BEST_OF_N" ]]; }                           # ok: full best-of-N sample
control_flat() {   # ok: every control value within NOISE_PCT of the first
  local ref="$1"; shift
  [[ -z "$ref" || "$ref" -le 0 ]] 2>/dev/null && return 1
  local v
  for v in "$@"; do
    awk -v r="$ref" -v c="$v" -v n="$NOISE_PCT" 'BEGIN{ d=(r>c?r-c:c-r); exit !(d*100.0/r <= n) }' || return 1
  done
  return 0
}
us_per_op() { awk -v r="${1:-0}" 'BEGIN{ if(r>0) printf "%.2f", 1000000.0/r; else printf "n/a" }'; }
ratio()     { awk -v m="${1:-0}" -v r="${2:-0}" 'BEGIN{ if(r>0) printf "%.3f", m/r; else printf "n/a" }'; }
# verdict <moon_rps> <redis_rps> -> WIN | LOSS | TIE  (tie-band = +/- NOISE_PCT around parity)
verdict() {
  awk -v m="${1:-0}" -v r="${2:-0}" -v n="$NOISE_PCT" 'BEGIN{
    if (r<=0 || m<=0) { print "n/a"; exit }
    hi=1.0+n/100.0; lo=1.0-n/100.0; x=m/r;
    if (x>=hi) print "WIN"; else if (x<=lo) print "LOSS"; else print "TIE";
  }'
}

# ============================================================================= self-test
self_test() {
  local fails=0
  _ok()  { printf '  ok   %s\n' "$1" >&2; }
  _bad() { printf '  FAIL %s\n' "$1" >&2; fails=$((fails+1)); }
  log "=== gcloud-kv-p1-baseline --self-test (gate + verdict logic; no GCloud) ==="

  gate_quiesce 1.40 && _bad "quiesce should VOID at load 1.40" || _ok "quiesce VOIDs high load"
  gate_quiesce 0.20 && _ok "quiesce passes low load" || _bad "quiesce should pass load 0.20"
  gate_steal 7 && _bad "steal should VOID at 7%" || _ok "steal VOIDs 7% (shared/contended)"
  gate_steal 0 && _ok "steal passes ~0% (dedicated)" || _bad "steal should pass 0%"
  gate_clean "planted-redis-server" && _bad "clean should refuse a planted proc" || _ok "clean refuses planted proc"
  require_floor 1 && _bad "require_floor should reject 1 rep" || _ok "require_floor rejects single rep"
  require_floor "$BEST_OF_N" && _ok "require_floor accepts full best-of-N" || _bad "require_floor should accept N"
  control_flat 100000 101000 99000 && _ok "control_flat passes within noise" || _bad "control_flat should pass"
  control_flat 100000 101000 70000 && _bad "control_flat should VOID a 30% drop" || _ok "control_flat VOIDs non-flat"
  [[ "$(us_per_op 100000)" == "10.00" ]] && _ok "us_per_op 100000 -> 10.00us" || _bad "us_per_op 100000 wrong ($(us_per_op 100000))"
  [[ "$(ratio 110000 100000)" == "1.100" ]] && _ok "ratio 110k/100k -> 1.100" || _bad "ratio wrong ($(ratio 110000 100000))"
  # verdict: the goal predicate. WIN only beyond the noise band.
  [[ "$(verdict 110000 100000)" == "WIN"  ]] && _ok "verdict 110k vs 100k -> WIN"  || _bad "verdict should WIN ($(verdict 110000 100000))"
  [[ "$(verdict 100000 110000)" == "LOSS" ]] && _ok "verdict 100k vs 110k -> LOSS" || _bad "verdict should LOSS ($(verdict 100000 110000))"
  [[ "$(verdict 103000 100000)" == "TIE"  ]] && _ok "verdict 103k vs 100k -> TIE (within noise)" || _bad "verdict should TIE ($(verdict 103000 100000))"
  [[ "$(verdict 100000 0)" == "n/a" ]] && _ok "verdict guards div-by-zero" || _bad "verdict should n/a on redis=0"
  grep -q 'p=1' "$0" && grep -q 'LOOPBACK' "$0" && _ok "harness declares p=1 loopback intent" || _bad "harness must declare intent"

  if [[ "$fails" -eq 0 ]]; then log "=== self-test PASS (all gates + verdict fail-closed correctly) ==="; return 0; fi
  log "=== self-test FAIL ($fails) ==="; return 1
}

# ============================================================================= measurement core
WORK="${WORK:-$HOME/moon-kv-p1}"
BINS="${BINS:-$HOME/moon-kv-p1-bins}"
REDIS_DIR="${REDIS_DIR:-$HOME/redis-$REDIS_VER}"
REDIS_SERVER="$REDIS_DIR/src/redis-server"
REDIS_BENCH="$REDIS_DIR/src/redis-benchmark"
REDIS_CLI="$REDIS_DIR/src/redis-cli"
MOON_PID=""; REDIS_PID=""
cleanup_moon()  { [[ -n "$MOON_PID"  ]] && { kill "$MOON_PID"  2>/dev/null||true; wait "$MOON_PID"  2>/dev/null||true; MOON_PID="";  }; return 0; }
cleanup_redis() { [[ -n "$REDIS_PID" ]] && { kill "$REDIS_PID" 2>/dev/null||true; wait "$REDIS_PID" 2>/dev/null||true; REDIS_PID=""; }; return 0; }
cleanup()       { cleanup_moon; cleanup_redis; return 0; }
current_steal() { LC_ALL=C vmstat 1 2 2>/dev/null | tail -1 | awk '{print $(NF)}'; }   # 'st' column
current_load()  { awk '{print $1}' /proc/loadavg; }
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

build_moon() {  # -> echoes binary path (read-only checkout; never edits src)
  local out="$BINS/moon-${MOON_REF//\//_}"
  [[ -x "$out" ]] && { echo "$out"; return 0; }
  git -C "$WORK" checkout -q "$MOON_REF" || die "checkout $MOON_REF"
  log "  moon built-from: $(git -C "$WORK" rev-parse --short HEAD) $(git -C "$WORK" log -1 --format=%s | head -c 50)"
  ( cd "$WORK" && CARGO_TARGET_DIR="$WORK/target" cargo build --release >/dev/null 2>&1 ) || die "moon build failed"
  mkdir -p "$BINS"; cp "$WORK/target/release/moon" "$out"
  # ELF-magic assert (Mach-O trap): first 4 bytes must be 7f 45 4c 46.
  local magic; magic=$(od -An -tx1 -N4 "$out" | tr -d ' ')
  [[ "$magic" == "7f454c46" ]] || die "moon binary is not ELF (magic=$magic) — stale Mach-O?"
  echo "$out"
}

build_redis() {  # canonical Redis from source (redis-server + redis-benchmark + redis-cli)
  [[ -x "$REDIS_SERVER" && -x "$REDIS_BENCH" ]] && return 0
  log "=== building Redis $REDIS_VER from source ==="
  ( cd "$HOME" && curl -fsSL "https://download.redis.io/releases/redis-${REDIS_VER}.tar.gz" -o "redis-${REDIS_VER}.tar.gz" \
      && tar xzf "redis-${REDIS_VER}.tar.gz" && cd "redis-${REDIS_VER}" && make -j"$(nproc)" BUILD_TLS=no >/dev/null 2>&1 ) \
      || die "redis build failed"
  [[ -x "$REDIS_SERVER" && -x "$REDIS_BENCH" ]] || die "redis binaries missing after build"
}

start_moon() {  # start_moon <bin>  (shard=1, pinned, loopback; PERSIST=memory|aof)
  local bin="$1"; cleanup_moon
  local dir; dir="$(mktemp -d /tmp/moon-kvp1.XXXXXX)"
  # Fair vs stock Redis. Moon defaults appendonly=yes AND disk-offload=enable; leaving offload on
  # makes Moon do cold-tier bookkeeping Redis has no analog for (KV review §117) -> always disable it.
  local pflags
  if [[ "$PERSIST" == aof ]]; then
    # AOF-on everysec, fair vs Redis --appendonly yes --appendfsync everysec. shards=1 => AOF-only
    # (per_shard_aof_active needs >=2) => no WAL double-write.
    pflags=(--appendonly yes --appendfsync everysec --disk-offload disable)
  else
    # pure in-memory: appendonly=no + no --save => persistence_dir=None => no WAL, no AOF (main.rs:765).
    pflags=(--appendonly no --disk-offload disable)
  fi
  # MOON_START_ENV: optional space-separated KEY=VAL env for the server process
  # (diagnostics, e.g. MOON_START_ENV="MOON_NO_URING=1" for an epoll-driver A/B).
  # MOON_SHARDS / SERVER_CORES / MOON_EXTRA_FLAGS: multi-shard cells
  # (--measure-shards) override shard count, core mask, and add flags such as
  # --io-busy-poll-us; defaults preserve the original shards=1 single-core path.
  # shellcheck disable=SC2086
  taskset -c "${SERVER_CORES:-$SERVER_CORE}" env ${MOON_START_ENV:-} "$bin" --port "$MOON_PORT" \
    --shards "${MOON_SHARDS:-1}" --dir "$dir" \
    "${pflags[@]}" ${MOON_EXTRA_FLAGS:-} --admin-port 0 >/dev/null 2>&1 &
  MOON_PID=$!
  local deadline=$((SECONDS+10))
  until "$REDIS_CLI" -p "$MOON_PORT" ping >/dev/null 2>&1; do
    kill -0 "$MOON_PID" 2>/dev/null || { log "  ERROR moon died on start"; return 1; }
    [[ $SECONDS -ge $deadline ]] && { log "  ERROR moon did not start"; return 1; }
    sleep 0.1
  done
  # io_uring self-report (once/run): monoio FusionDriver silently falls back to epoll if io_uring is
  # unavailable and logs NEITHER choice -> assert the driver empirically via /proc/<pid>/fd. A live
  # io_uring driver shows as anon_inode:[io_uring]. Absence => epoll fallback => p=1 numbers are a
  # driver confound, not an arch effect (ruled out on aarch64 proxy 2026-07-03; c4a self-confirms here).
  if [[ -z "${URING_REPORTED:-}" ]]; then
    URING_REPORTED=1
    if ls -l "/proc/$MOON_PID/fd" 2>/dev/null | grep -q 'anon_inode:\[io_uring\]'; then
      log "  moon io_uring driver: ACTIVE (anon_inode:[io_uring] fd present)"
    else
      log "  moon io_uring driver: NOT FOUND (epoll fallback?) — treat p=1 as a driver confound"
    fi
  fi
}

start_redis() {  # canonical Redis, pinned, loopback; PERSIST=memory|aof
  cleanup_redis
  local pflags
  if [[ "$PERSIST" == aof ]]; then
    local rdir; rdir="$(mktemp -d /tmp/redis-kvp1.XXXXXX)"
    pflags=(--appendonly yes --appendfsync everysec --dir "$rdir" --save '')
  else
    pflags=(--appendonly no --save '')
  fi
  taskset -c "$SERVER_CORE" "$REDIS_SERVER" --port "$REDIS_PORT" \
    "${pflags[@]}" --protected-mode no --daemonize no >/dev/null 2>&1 &
  REDIS_PID=$!
  local deadline=$((SECONDS+10))
  until "$REDIS_CLI" -p "$REDIS_PORT" ping >/dev/null 2>&1; do
    kill -0 "$REDIS_PID" 2>/dev/null || { log "  ERROR redis died on start"; return 1; }
    [[ $SECONDS -ge $deadline ]] && { log "  ERROR redis did not start"; return 1; }
    sleep 0.1
  done
}

bench_pair() {  # bench_pair <port> -> "SET_rps|GET_rps" (real keyspace, SET-then-GET hit-path)
  # -c/-P come from BENCH_CLIENTS/BENCH_PIPE (default 1/1 = the p=1 single-op path).
  # BENCH_THREADS (optional): client threads for high-throughput multi-shard
  # cells where a single client thread would be the bottleneck.
  # shellcheck disable=SC2086
  local out; out=$(taskset -c "$CLIENT_CORE" "$REDIS_BENCH" -p "$1" -c "$BENCH_CLIENTS" -P "$BENCH_PIPE" -n "$REQUESTS" \
      ${BENCH_THREADS:+--threads "$BENCH_THREADS"} \
      -r "$KEYSPACE" -t set,get --csv 2>/dev/null | tr '\r' '\n')
  local s g
  s=$(printf '%s\n' "$out" | grep '"SET"' | awk -F',' '{gsub(/"/,"",$2); printf "%.0f\n",$2}' | tail -1)
  g=$(printf '%s\n' "$out" | grep '"GET"' | awk -F',' '{gsub(/"/,"",$2); printf "%.0f\n",$2}' | tail -1)
  printf '%s|%s\n' "${s:-0}" "${g:-0}"
}

# cell_engine <engine> <bin_or_-> -> emits "engine|SET|best|reps|n" and "engine|GET|best|reps|n"
cell_engine() {
  local engine="$1" bin="$2" svals=() gvals=() n=0 attempts=0 pair s g
  local max_attempts=$((BEST_OF_N*3))
  while [[ "$n" -lt "$BEST_OF_N" && "$attempts" -lt "$max_attempts" ]]; do
    attempts=$((attempts+1))
    wait_quiesced
    if [[ "$engine" == "moon" ]]; then start_moon "$bin" || { sleep 2; continue; }
    else start_redis || { sleep 2; continue; }; fi
    pair="$(bench_pair "$([[ "$engine" == moon ]] && echo "$MOON_PORT" || echo "$REDIS_PORT")")"
    cleanup
    s="${pair%%|*}"; g="${pair##*|}"
    [[ -z "$s" || "$s" -le 0 || -z "$g" || "$g" -le 0 ]] 2>/dev/null && { log "  retry $engine (bad rep: '$pair')"; continue; }
    svals+=("$s"); gvals+=("$g"); n=$((n+1))
  done
  local sbest=0 gbest=0
  [[ "$n" -gt 0 ]] && sbest=$(printf '%s\n' "${svals[@]}" | sort -n | tail -1)
  [[ "$n" -gt 0 ]] && gbest=$(printf '%s\n' "${gvals[@]}" | sort -n | tail -1)
  printf '%s|SET|%s|%s|%s\n' "$engine" "$sbest" "$(IFS=,;echo "${svals[*]:-}")" "$n"
  printf '%s|GET|%s|%s|%s\n' "$engine" "$gbest" "$(IFS=,;echo "${gvals[*]:-}")" "$n"
}

measure() {
  command -v taskset >/dev/null || die "taskset not available"
  prepare_repo
  build_redis
  trap cleanup EXIT INT TERM

  local steal; steal=$(current_steal); steal="${steal:-0}"
  if ! gate_steal "$steal"; then
    printf 'status: VOID\nreason: contended_instrument\ndetail: vCPU steal %%=%s > %s (not dedicated)\n' "$steal" "$STEAL_MAX"
    die "VOID contended_instrument (steal=$steal%)"
  fi
  if ! gate_clean; then
    printf 'status: VOID\nreason: dirty_instrument\ndetail: a stray moon/redis process is alive\n'
    die "VOID dirty_instrument"
  fi
  log "=== gates OK (steal=$steal%, clean) — measuring on $(uname -srm) ==="

  local moonbin; moonbin=$(build_moon) || die "moon build"
  log "  settling compile heat..."; wait_quiesced

  echo "# kv-p1-baseline raw (loopback, shard=1, c1, P1, SET-then-GET hit-path)"
  echo "# machine|engine|cmd|best_rps|us_per_op|reps|n"
  declare -A BEST N
  local engine line eng cmd best reps nn
  for engine in moon redis; do
    while IFS='|' read -r eng cmd best reps nn; do
      BEST["$eng|$cmd"]="$best"; N["$eng|$cmd"]="$nn"
      printf '%s|%s|%s|%s|%s|%s|%s\n' "$GCE_MACHINE" "$eng" "$cmd" "$best" "$(us_per_op "$best")" "$reps" "$nn"
    done < <( cell_engine "$engine" "${moonbin:-"-"}" )
  done

  # validity: full sample on all four cells
  local c
  for c in "moon|SET" "moon|GET" "redis|SET" "redis|GET"; do
    require_floor "${N[$c]:-0}" || { printf 'status: VOID\nreason: unstable_sample\ndetail: %s only %s reps\n' "$c" "${N[$c]:-0}"; die "VOID unstable_sample ($c)"; }
  done

  echo "# verdict (Moon vs Redis, p=1 c1, dedicated loopback):"
  echo "# machine|cmd|moon_rps|redis_rps|moon_us|redis_us|ratio|verdict"
  for cmd in SET GET; do
    local m="${BEST["moon|$cmd"]:-0}" r="${BEST["redis|$cmd"]:-0}"
    printf '%s|%s|%s|%s|%s|%s|%s|%s\n' "$GCE_MACHINE" "$cmd" "$m" "$r" \
      "$(us_per_op "$m")" "$(us_per_op "$r")" "$(ratio "$m" "$r")" "$(verdict "$m" "$r")"
  done
}

# measure_driver: SAME-INSTANCE driver A/B — moon(io_uring) vs moon(epoll via MOON_NO_URING=1,
# requires the monoio LegacyDriver honor-fix) vs redis control. Same-instance kills the ~2.5%
# c4a instance-to-instance variance that makes two-instance driver deltas unreadable.
measure_driver() {
  command -v taskset >/dev/null || die "taskset not available"
  prepare_repo
  build_redis
  trap cleanup EXIT INT TERM

  local steal; steal=$(current_steal); steal="${steal:-0}"
  gate_steal "$steal" || { printf 'status: VOID\nreason: contended_instrument\n'; die "VOID steal=$steal%"; }
  gate_clean || { printf 'status: VOID\nreason: dirty_instrument\n'; die "VOID dirty"; }
  log "=== gates OK (steal=$steal%, clean) — driver A/B on $(uname -srm) ==="

  local moonbin; moonbin=$(build_moon) || die "moon build"
  log "  settling compile heat..."; wait_quiesced

  echo "# kv-p1 driver A/B raw (same instance: moon-uring vs moon-epoll vs redis control)"
  echo "# machine|variant|cmd|best_rps|us_per_op|reps|n"
  declare -A BEST N
  # Caller-provided MOON_START_ENV (e.g. MOON_URING_SPIN_US=40) composes into
  # the moon cells instead of being clobbered by the per-cell driver forcing —
  # the uring cell is "caller env as-is", the epoll cell adds MOON_NO_URING=1.
  local base_env="${MOON_START_ENV:-}"
  local eng cmd best reps nn variant vbin veng
  for variant in uring epoll redis; do
    case "$variant" in
      uring) MOON_START_ENV="$base_env"; URING_REPORTED=""; veng=moon; vbin="$moonbin" ;;
      epoll) MOON_START_ENV="$base_env MOON_NO_URING=1"; URING_REPORTED=""; veng=moon; vbin="$moonbin" ;;
      redis) veng=redis; vbin="-" ;;
    esac
    while IFS='|' read -r eng cmd best reps nn; do
      BEST["$variant|$cmd"]="$best"; N["$variant|$cmd"]="$nn"
      printf '%s|%s|%s|%s|%s|%s|%s\n' "$GCE_MACHINE" "$variant" "$cmd" "$best" "$(us_per_op "$best")" "$reps" "$nn"
    done < <( cell_engine "$veng" "$vbin" )
  done

  local c
  for c in "uring|SET" "uring|GET" "epoll|SET" "epoll|GET" "redis|SET" "redis|GET"; do
    require_floor "${N[$c]:-0}" || { printf 'status: VOID\nreason: unstable_sample\ndetail: %s only %s reps\n' "$c" "${N[$c]:-0}"; die "VOID unstable_sample ($c)"; }
  done

  echo "# verdict (same-instance driver A/B):"
  echo "# machine|cmd|uring_rps|epoll_rps|redis_rps|uring/redis|epoll/redis|epoll/uring"
  for cmd in SET GET; do
    local u="${BEST["uring|$cmd"]:-0}" e="${BEST["epoll|$cmd"]:-0}" r="${BEST["redis|$cmd"]:-0}"
    printf '%s|%s|%s|%s|%s|%s|%s|%s\n' "$GCE_MACHINE" "$cmd" "$u" "$e" "$r" \
      "$(ratio "$u" "$r")" "$(ratio "$e" "$r")" "$(ratio "$e" "$u")"
  done
}

# measure_diag: on-instance perf ATTRIBUTION (not a verdict) — flat perf profiles,
# syscalls + ctx-switches for moon(epoll, debug-symbol build) vs redis under
# sustained p=1 c1 GET load. Answers "where do the remaining ~µs/op go on THIS
# machine". Requires linux-tools (installed by provisioning).
measure_diag() {
  command -v taskset >/dev/null || die "taskset not available"
  command -v perf >/dev/null || die "perf not installed (linux-tools)"
  prepare_repo
  build_redis
  trap cleanup EXIT INT TERM

  local steal; steal=$(current_steal); steal="${steal:-0}"
  gate_steal "$steal" || die "VOID steal=$steal%"
  gate_clean || die "VOID dirty"
  log "=== gates OK (steal=$steal%) — perf attribution on $(uname -srm) ==="

  # Debug-symbol moon build (frame pointers for perf -g); separate cache name so
  # it never contaminates the verdict-mode generic build.
  local dbin="$BINS/moon-diag-${MOON_REF//\//_}"
  if [[ ! -x "$dbin" ]]; then
    git -C "$WORK" checkout -q "$MOON_REF" || die "checkout $MOON_REF"
    ( cd "$WORK" && CARGO_TARGET_DIR="$WORK/target-diag" CARGO_PROFILE_RELEASE_DEBUG=true \
        CARGO_PROFILE_RELEASE_STRIP=none RUSTFLAGS="-C force-frame-pointers=yes" \
        cargo build --release >/dev/null 2>&1 ) || die "diag moon build failed"
    mkdir -p "$BINS"; cp "$WORK/target-diag/release/moon" "$dbin"
  fi
  local magic; magic=$(od -An -tx1 -N4 "$dbin" | tr -d ' ')
  [[ "$magic" == "7f454c46" ]] || die "diag binary not ELF (magic=$magic)"

  echo "# p=1 perf attribution: moon(epoll driver, debug syms) vs redis, sustained c1 GET"
  local engine pid port bpid rps
  for engine in moon redis; do
    wait_quiesced
    if [[ "$engine" == moon ]]; then
      MOON_START_ENV="MOON_NO_URING=1" URING_REPORTED="" start_moon "$dbin" || die "moon start"
      pid=$MOON_PID; port=$MOON_PORT
    else
      start_redis || die "redis start"
      pid=$REDIS_PID; port=$REDIS_PORT
    fi
    taskset -c "$CLIENT_CORE" "$REDIS_BENCH" -p "$port" -t set,get -n 30000 -c 1 -P 1 -r "$KEYSPACE" -q >/dev/null 2>&1
    taskset -c "$CLIENT_CORE" "$REDIS_BENCH" -p "$port" -t get -n 3000000 -c 1 -P 1 -r "$KEYSPACE" -q \
      > "/tmp/diagbench-$engine.txt" 2>&1 &
    bpid=$!
    sleep 2
    sudo perf stat -e task-clock,context-switches,raw_syscalls:sys_enter -p "$pid" \
      -o "/tmp/diagstat-$engine.txt" -- sleep 6 2>/dev/null || true
    sudo perf record -F 997 -g -p "$pid" -o "/tmp/diagperf-$engine.data" -- sleep 8 2>/dev/null || true
    kill "$bpid" 2>/dev/null; wait "$bpid" 2>/dev/null
    cleanup
    echo "== $engine =="
    echo "-- perf stat (6s window; ctx-switches ~= ops at p=1) --"
    grep -E 'task-clock|context-switches|sys_enter' "/tmp/diagstat-$engine.txt" 2>/dev/null
    echo "-- flat self% top 30 --"
    sudo perf report -i "/tmp/diagperf-$engine.data" --stdio --no-children -g none \
      --percent-limit 0.4 2>/dev/null | grep -E "^\s+[0-9]" | head -30
  done
}

# measure_tput: rigorous throughput sweep (clients x pipeline-depth), reusing the p=1 rigor
# (pinned disjoint cores, steal gate, fresh-server best-of-N). Emits ratio + verdict per cell so
# "faster per core at depth" is a clean claim. SET vs GET separated -> exposes read/write asymmetry.
measure_tput() {
  command -v taskset >/dev/null || die "taskset not available"
  prepare_repo
  build_redis
  trap cleanup EXIT INT TERM

  local steal; steal=$(current_steal); steal="${steal:-0}"
  gate_steal "$steal" || { printf 'status: VOID\nreason: contended_instrument\ndetail: steal=%s > %s\n' "$steal" "$STEAL_MAX"; die "VOID contended (steal=$steal%)"; }
  gate_clean       || { printf 'status: VOID\nreason: dirty_instrument\n'; die "VOID dirty_instrument"; }
  log "=== gates OK (steal=$steal%, clean) — throughput sweep on $(uname -srm) ==="

  local moonbin; moonbin=$(build_moon) || die "moon build"
  log "  settling compile heat..."; wait_quiesced

  echo "# kv-throughput raw (loopback, shard=1, pinned, best-of-$BEST_OF_N, real keyspace, SET-then-GET)"
  echo "# persist=$PERSIST clients=[$CLIENTS_SWEEP] pipes=[$PIPES] requests=$REQUESTS keyspace=$KEYSPACE"
  echo "# machine|clients|pipe|cmd|moon_rps|redis_rps|moon_us|redis_us|ratio|verdict"
  declare -A BEST N
  local C P engine eng cmd best reps nn cell
  for C in $CLIENTS_SWEEP; do
    for P in $PIPES; do
      BENCH_CLIENTS="$C"; BENCH_PIPE="$P"
      BEST=(); N=()
      for engine in moon redis; do
        while IFS='|' read -r eng cmd best reps nn; do
          BEST["$eng|$cmd"]="$best"; N["$eng|$cmd"]="$nn"
        done < <( cell_engine "$engine" "${moonbin:-"-"}" )
      done
      for cell in "moon|SET" "moon|GET" "redis|SET" "redis|GET"; do
        require_floor "${N[$cell]:-0}" || log "  WARN c=$C P=$P $cell only ${N[$cell]:-0}/$BEST_OF_N reps"
      done
      for cmd in SET GET; do
        local m="${BEST["moon|$cmd"]:-0}" r="${BEST["redis|$cmd"]:-0}"
        printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' "$GCE_MACHINE" "$C" "$P" "$cmd" "$m" "$r" \
          "$(us_per_op "$m")" "$(us_per_op "$r")" "$(ratio "$m" "$r")" "$(verdict "$m" "$r")"
      done
    done
  done
}

# measure_shards: shard-scaling sweep with/without --io-busy-poll-us, same rigor
# (pinned, steal-gated, fresh-server best-of-N, strict keyspace so multi-shard
# actually scatters cross-shard). Needs an 8-vCPU instance: moon shards pin to
# cores 0..3, the client to 5-7, Redis control to core 0. Cells:
#   shards x spin x { c1/P1 (single-conn latency, incl. the cross-shard-hop
#   story at shards=4), c8/P1, c8/P16, c8/P64 }.
# Redis is measured once per client/pipe combo (single-threaded canonical).
measure_shards() {
  command -v taskset >/dev/null || die "taskset not available"
  prepare_repo
  build_redis
  trap cleanup EXIT INT TERM

  local steal; steal=$(current_steal); steal="${steal:-0}"
  gate_steal "$steal" || { printf 'status: VOID\nreason: contended_instrument\n'; die "VOID contended (steal=$steal%)"; }
  gate_clean       || { printf 'status: VOID\nreason: dirty_instrument\n'; die "VOID dirty_instrument"; }
  log "=== gates OK (steal=$steal%, clean) — shard sweep on $(uname -srm) ==="

  local moonbin; moonbin=$(build_moon) || die "moon build"
  log "  settling compile heat..."; wait_quiesced

  local combos=( "1|1" "8|1" "8|16" "8|64" )
  CLIENT_CORE="${SHARD_CLIENT_CORES:-5-7}"
  echo "# kv-shard-scaling raw (loopback, pinned: client cores $CLIENT_CORE, best-of-$BEST_OF_N, strict keyspace)"
  echo "# persist=$PERSIST requests=$REQUESTS keyspace=$KEYSPACE shards=[$SHARDS_SWEEP] spin=[$SPIN_SWEEP]"
  echo "# machine|shards|spin_us|clients|pipe|cmd|moon_rps|redis_rps|ratio|verdict"

  declare -A RBEST MB
  local combo C P eng cmd best reps nn S SPIN m r
  # Redis control per combo (shards/spin don't apply to it)
  for combo in "${combos[@]}"; do
    C="${combo%%|*}"; P="${combo##*|}"
    BENCH_CLIENTS="$C"; BENCH_PIPE="$P"
    BENCH_THREADS=""; [[ "$C" -gt 1 ]] && BENCH_THREADS=3
    while IFS='|' read -r eng cmd best reps nn; do
      RBEST["$C|$P|$cmd"]="$best"
    done < <( cell_engine redis "-" )
  done
  for S in $SHARDS_SWEEP; do
    for SPIN in $SPIN_SWEEP; do
      MOON_SHARDS="$S"
      SERVER_CORES="0"; [[ "$S" -gt 1 ]] && SERVER_CORES="0-$((S-1))"
      MOON_EXTRA_FLAGS=""; [[ "$SPIN" != 0 ]] && MOON_EXTRA_FLAGS="--io-busy-poll-us $SPIN"
      for combo in "${combos[@]}"; do
        C="${combo%%|*}"; P="${combo##*|}"
        BENCH_CLIENTS="$C"; BENCH_PIPE="$P"
        BENCH_THREADS=""; [[ "$C" -gt 1 ]] && BENCH_THREADS=3
        MB=()
        while IFS='|' read -r eng cmd best reps nn; do
          MB["$cmd"]="$best"
        done < <( cell_engine moon "$moonbin" )
        for cmd in SET GET; do
          m="${MB[$cmd]:-0}"; r="${RBEST["$C|$P|$cmd"]:-0}"
          printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' "$GCE_MACHINE" "$S" "$SPIN" "$C" "$P" "$cmd" \
            "$m" "$r" "$(ratio "$m" "$r")" "$(verdict "$m" "$r")"
        done
      done
    done
  done
  # reset overrides so any later mode in the same process gets shards=1 defaults
  MOON_SHARDS=""; SERVER_CORES=""; MOON_EXTRA_FLAGS=""; BENCH_THREADS=""
}

# ============================================================================= gcloud orchestration
arch_image_family() {  # ubuntu image family for the machine's arch
  case "$1" in
    c4a-*|t2a-*|*-arm*|*arm64*) echo "ubuntu-2404-lts-arm64" ;;
    *) echo "ubuntu-2404-lts-amd64" ;;
  esac
}

disk_type_for() {  # boot-disk type by machine family. c4a/c4/n4/*-metal are Hyperdisk-ONLY
  case "$1" in    # (pd-ssd is rejected at create); c3 & older still take pd-ssd.
    c4a-*|c4-*|n4-*|*-metal) echo "hyperdisk-balanced" ;;
    *) echo "pd-ssd" ;;
  esac
}

gcloud_run_one() {
  command -v gcloud >/dev/null || die "gcloud CLI not found"
  local imgfam; imgfam=$(arch_image_family "$GCE_MACHINE")
  local disktype; disktype=$(disk_type_for "$GCE_MACHINE")
  local GSSH=(gcloud compute ssh "$GCE_NAME" --zone="$GCE_ZONE" --quiet
        --ssh-flag=-oStrictHostKeyChecking=no --ssh-flag=-oConnectTimeout=15)
  # Leak backstop (design-for-failure): GCE auto-DELETEs the VM after MAX_RUN even if this
  # orchestrator is killed mid-run (an unattended background job here can be reaped). Standard-VM
  # limited-runtime form; no --provisioning-model needed. Toggle off with SELF_DESTRUCT=0.
  local guard=(--max-run-duration="${MAX_RUN:-45m}" --instance-termination-action=DELETE)
  [[ "${SELF_DESTRUCT:-1}" == 1 ]] || guard=()
  log "=== provisioning $GCE_NAME ($GCE_MACHINE, $GCE_ZONE, $imgfam, $disktype; guard=${guard[*]:-off}) ==="
  gcloud compute instances create "$GCE_NAME" \
    --machine-type="$GCE_MACHINE" --zone="$GCE_ZONE" \
    --image-family="$imgfam" --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB --boot-disk-type="$disktype" \
    "${guard[@]}" --quiet \
    || die "instance create failed"

  # Pin the actual name into the teardown trap NOW (env-prefix reverts GCE_NAME at fire time).
  local _inst="$GCE_NAME" _zone="$GCE_ZONE"
  trap "log '=== tearing down $_inst ==='; gcloud compute instances delete '$_inst' --zone='$_zone' -q 2>/dev/null || true" EXIT INT TERM

  log "=== waiting for SSH ==="
  local tries=0
  until "${GSSH[@]}" --command='echo up' >/dev/null 2>&1; do
    tries=$((tries+1)); [[ $tries -ge 40 ]] && die "SSH never came up"; sleep 5
  done

  log "=== provisioning toolchain + repo on the instance ==="
  "${GSSH[@]}" --command='
    set -e
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential pkg-config libssl-dev git curl ca-certificates
    # perf for --measure-diag (kernel-matched first, gcp/generic fallback; non-fatal)
    sudo apt-get install -y -qq linux-tools-$(uname -r) 2>/dev/null \
      || sudo apt-get install -y -qq linux-tools-gcp 2>/dev/null \
      || sudo apt-get install -y -qq linux-tools-generic 2>/dev/null || true
    command -v cargo >/dev/null || curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1
  ' || die "instance provisioning failed"

  log "=== pushing harness ==="
  gcloud compute scp scripts/gcloud-kv-p1-baseline.sh "$GCE_NAME":~/gcloud-kv-p1-baseline.sh \
    --zone="$GCE_ZONE" --quiet --scp-flag=-oStrictHostKeyChecking=no || die "scp failed"

  local mcmd="${MEASURE_CMD:---measure}"
  local rawfile="${RAWFILE:-tmp/kv-p1-${GCE_MACHINE}.txt}"
  log "=== running $mcmd on $GCE_MACHINE (build + bench; the long part) -> $rawfile ==="
  "${GSSH[@]}" --command="
    source \$HOME/.cargo/env
    GCE_MACHINE='$GCE_MACHINE' MOON_REF='$MOON_REF' REDIS_VER='$REDIS_VER' BEST_OF_N='$BEST_OF_N' \
    REQUESTS='$REQUESTS' KEYSPACE='$KEYSPACE' PIPES='$PIPES' CLIENTS_SWEEP='$CLIENTS_SWEEP' PERSIST='$PERSIST' \
    SHARDS_SWEEP='$SHARDS_SWEEP' SPIN_SWEEP='$SPIN_SWEEP' SHARD_CLIENT_CORES='$SHARD_CLIENT_CORES' \
    MOON_SHARDS='${MOON_SHARDS:-}' SERVER_CORES='${SERVER_CORES:-}' MOON_EXTRA_FLAGS='${MOON_EXTRA_FLAGS:-}' \
    CLIENT_CORE='${CLIENT_CORE_OVERRIDE:-$CLIENT_CORE}' \
    MOON_START_ENV='${MOON_START_ENV:-}' \
      bash ~/gcloud-kv-p1-baseline.sh $mcmd
  " | tee "$rawfile"

  log "=== $GCE_MACHINE raw results in $rawfile; teardown follows (trap) ==="
}

gcloud_sweep() {
  log "=== KV-P1 SWEEP: $MACHINES ==="
  local m short
  for m in $MACHINES; do
    short="${m%%-*}"
    log ""; log "##################### machine: $m #####################"
    ( GCE_MACHINE="$m"; GCE_NAME="moon-kv-p1-${short}"; gcloud_run_one ) \
      || log "WARN: machine $m run failed (see log); continuing"
  done
  log "=== sweep complete; per-machine raw files: tmp/kv-p1-<machine>.txt ==="
}

# ============================================================================= dispatch
case "${1:---gcloud}" in
  --self-test)    self_test ;;
  --measure)      measure ;;
  --measure-driver) measure_driver ;;
  --measure-diag) measure_diag ;;
  --measure-shards) measure_shards ;;
  --measure-tput) measure_tput ;;
  --gcloud)       gcloud_sweep ;;
  --one)          gcloud_run_one ;;
  *) die "unknown subcommand: $1 (use --self-test | --measure | --measure-driver | --measure-tput | --gcloud | --one)" ;;
esac
