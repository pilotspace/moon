#!/usr/bin/env bash
# gcloud-kv-scale-bench.sh — Moon vs Redis at LARGE SCALE on GCloud bare-metal, cross-arch.
#
# The p=1 baseline (gcloud-kv-p1-baseline.sh) answers "faster at single-op latency". This answers the
# OTHER two dimensions of the 2x2 proof the user asked for:
#   * FASTER  @ scale  -> scripts/bench-compare.sh   (pipeline 1..128, connection 1..500, -r large keyspace)
#   * LIGHTER @ scale  -> scripts/bench-resources.sh (RSS-per-key, fresh-server-per-row)
# It REUSES those two debugged measurement scripts (advisor: reuse beats rebuild); this file is only the
# GCloud provisioning + fair-config glue, patterned on gcloud-kv-p1-baseline.sh.
#
# Fairness (both traps closed):
#   * bench-compare.sh already starts BOTH engines AOF-off.
#   * bench-resources.sh FIXED to start Moon --appendonly no --disk-offload disable (was AOF-on -> RSS-unfair).
#   * Moon --shards 1: fair per-key memory (no per-shard baseline) + strongest non-crossshard throughput.
#   * Canonical builds (cargo build --release ; redis make). ELF-magic assert. Dedicated vCPU, steal-gated.
#   * Instance clones shipped `main` for the MOON binary, but uses the LOCAL (fixed) bench scripts (scp'd
#     over the clone) so the fairness fixes apply even before they are committed.
#
# Measure-only: never edits moon/src. Subcommands:
#   --self-test      gate + parse logic vs synthetic inputs (NO GCloud, NO cost)
#   --measure-scale  INNER; assumes it runs ON a provisioned Linux instance
#   --gcloud         (default) OUTER sweep over $MACHINES: provision/build/measure/teardown
#   --one            OUTER for a single $GCE_MACHINE
set -uo pipefail

MOON_REF="${MOON_REF:-main}"
REDIS_VER="${REDIS_VER:-7.4.2}"
REQUESTS="${REQUESTS:-200000}"       # bench-compare per-test request count (stable numbers)
KEYSPACE="${KEYSPACE:-10000000}"     # -r large keyspace for the scale claim (10M)
SHARDS="${SHARDS:-1}"
STEAL_MAX="${STEAL_MAX:-1}"          # dedicated-vCPU steal gate (~0 on dedicated cores)
GCE_MACHINE="${GCE_MACHINE:-c3-standard-4}"
GCE_NAME="${GCE_NAME:-moon-kv-scale}"
GCE_ZONE="${GCE_ZONE:-us-central1-a}"
REPO_URL="${REPO_URL:-https://github.com/pilotspace/moon.git}"
MACHINES="${MACHINES:-c3-standard-4}"
WORK="${WORK:-$HOME/moon-kv-scale}"
REDIS_DIR="${REDIS_DIR:-$HOME/redis-$REDIS_VER}"

log(){ printf '%s\n' "$*" >&2; }
die(){ log "FATAL: $*"; exit 1; }

gate_steal()    { awk -v a="$1" -v b="$STEAL_MAX" 'BEGIN{exit !(a<=b)}'; }   # ok: steal <= STEAL_MAX
current_steal() { LC_ALL=C vmstat 1 2 2>/dev/null | tail -1 | awk '{print $(NF)}'; }  # 'st' column

# ============================================================================= self-test (no GCloud)
self_test(){
  local fails=0
  _ok(){  printf '  ok   %s\n' "$1" >&2; }
  _bad(){ printf '  FAIL %s\n' "$1" >&2; fails=$((fails+1)); }
  log "=== gcloud-kv-scale-bench --self-test (gates + parse + intent; no GCloud) ==="

  gate_steal 7 && _bad "steal should VOID at 7%" || _ok "steal VOIDs 7% (contended)"
  gate_steal 0 && _ok "steal passes 0% (dedicated)" || _bad "steal should pass 0%"

  # throughput row parse: a bench-compare table row -> redis($3), moon($4)
  local row="| SET p=128                      |       500000 |      1400000 |   2.80x |"
  local r m
  r=$(printf '%s' "$row" | awk -F'|' '{gsub(/ /,"",$3);print $3}')
  m=$(printf '%s' "$row" | awk -F'|' '{gsub(/ /,"",$4);print $4}')
  [[ "$r" == "500000" && "$m" == "1400000" ]] && _ok "throughput row parse (redis=$r moon=$m -> 2.8x)" \
    || _bad "throughput row parse ($r/$m)"

  # ratio>1 means moon faster; <1 slower
  awk 'BEGIN{exit !(1400000/500000 > 1)}' && _ok "ratio>1 => moon faster" || _bad "ratio math"

  grep -q 'measure-scale' "$0" && grep -q 'bench-resources' "$0" && grep -q 'bench-compare' "$0" \
    && _ok "declares scale (throughput+memory) intent" || _bad "must declare intent"

  [[ "$fails" -eq 0 ]] && { log "=== self-test PASS ==="; return 0; }
  log "=== self-test FAIL ($fails) ==="; return 1
}

# ============================================================================= inner: on the instance
measure_scale(){
  command -v git >/dev/null || die "git missing"
  local steal; steal=$(current_steal); steal="${steal:-0}"
  gate_steal "$steal" || die "VOID contended_instrument (steal=$steal% > $STEAL_MAX)"
  log "=== gates OK (steal=$steal%) on $(uname -srm) ==="

  # moon source = shipped ref; override bench scripts with the scp'd LOCAL (fixed) versions
  [[ -d "$WORK/.git" ]] || git clone -q "$REPO_URL" "$WORK" || die "clone failed"
  git -C "$WORK" fetch --all -q 2>/dev/null || true
  git -C "$WORK" checkout -q "$MOON_REF" || die "checkout $MOON_REF"
  [[ -f "$HOME/bench-compare.sh"   ]] && cp "$HOME/bench-compare.sh"   "$WORK/scripts/bench-compare.sh"
  [[ -f "$HOME/bench-resources.sh" ]] && cp "$HOME/bench-resources.sh" "$WORK/scripts/bench-resources.sh"
  chmod +x "$WORK/scripts/"*.sh 2>/dev/null || true
  log "  moon source: $(git -C "$WORK" rev-parse --short HEAD) $(git -C "$WORK" log -1 --format=%s | head -c 48)"

  # build moon (release) + ELF assert (Mach-O trap)
  ( cd "$WORK" && CARGO_TARGET_DIR="$WORK/target" cargo build --release >/dev/null 2>&1 ) || die "moon build failed"
  local magic; magic=$(od -An -tx1 -N4 "$WORK/target/release/moon" | tr -d ' ')
  [[ "$magic" == "7f454c46" ]] || die "moon not ELF (magic=$magic) — stale Mach-O?"

  # build redis from source, put on PATH
  if [[ ! -x "$REDIS_DIR/src/redis-server" ]]; then
    log "=== building Redis $REDIS_VER from source ==="
    ( cd "$HOME" && curl -fsSL "https://download.redis.io/releases/redis-${REDIS_VER}.tar.gz" -o redis.tgz \
        && tar xzf redis.tgz && cd "redis-${REDIS_VER}" && make -j"$(nproc)" BUILD_TLS=no >/dev/null 2>&1 ) \
        || die "redis build failed"
  fi
  export PATH="$REDIS_DIR/src:$PATH"
  { command -v redis-server >/dev/null && command -v redis-benchmark >/dev/null && command -v redis-cli >/dev/null; } \
    || die "redis binaries not on PATH"

  cd "$WORK"
  echo "# ===== KV SCALE BENCH  ($GCE_MACHINE, $(uname -m), shards=$SHARDS, -r $KEYSPACE) ====="
  echo "# moon: $(git rev-parse --short HEAD)   redis: $REDIS_VER   reqs/test: $REQUESTS   host: $(uname -srm)"
  echo
  echo "## ========== DIMENSION 2: FASTER (throughput @ scale) =========="
  KEYSPACE="$KEYSPACE" bash scripts/bench-compare.sh --shards "$SHARDS" --requests "$REQUESTS" \
    || log "WARN bench-compare exited $?"
  echo
  echo "## ========== DIMENSION 1: LIGHTER (memory @ scale) =========="
  bash scripts/bench-resources.sh --shards "$SHARDS" --skip-build --quick --output "$WORK/_res.md" >/dev/null 2>&1 \
    || log "WARN bench-resources exited $?"
  cat "$WORK/_res.md" 2>/dev/null || echo "(memory report missing)"
  echo "# ===== END SCALE BENCH ====="
}

# ============================================================================= gcloud orchestration
arch_image_family(){ case "$1" in c4a-*|t2a-*|*arm*|*arm64*) echo ubuntu-2404-lts-arm64;; *) echo ubuntu-2404-lts-amd64;; esac; }

gcloud_run_one(){
  command -v gcloud >/dev/null || die "gcloud CLI not found"
  local imgfam; imgfam=$(arch_image_family "$GCE_MACHINE")
  local GSSH=(gcloud compute ssh "$GCE_NAME" --zone="$GCE_ZONE" --quiet
        --ssh-flag=-oStrictHostKeyChecking=no --ssh-flag=-oConnectTimeout=15)
  log "=== provisioning $GCE_NAME ($GCE_MACHINE, $GCE_ZONE, $imgfam) ==="
  gcloud compute instances create "$GCE_NAME" \
    --machine-type="$GCE_MACHINE" --zone="$GCE_ZONE" \
    --image-family="$imgfam" --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB --boot-disk-type=pd-ssd --quiet || die "instance create failed"

  # Pin the actual name into the teardown trap NOW (env-prefix reverts GCE_NAME at fire time).
  local _inst="$GCE_NAME" _zone="$GCE_ZONE"
  trap "log '=== tearing down $_inst ==='; gcloud compute instances delete '$_inst' --zone='$_zone' -q 2>/dev/null || true" EXIT INT TERM

  log "=== waiting for SSH ==="
  local tries=0
  until "${GSSH[@]}" --command='echo up' >/dev/null 2>&1; do
    tries=$((tries+1)); [[ $tries -ge 40 ]] && die "SSH never came up"; sleep 5
  done

  log "=== provisioning toolchain (bc needed by bench-resources) ==="
  "${GSSH[@]}" --command='
    set -e
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential pkg-config libssl-dev git curl ca-certificates bc
    command -v cargo >/dev/null || curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1
  ' || die "instance provisioning failed"

  log "=== pushing harness + FIXED bench scripts ==="
  gcloud compute scp scripts/gcloud-kv-scale-bench.sh scripts/bench-compare.sh scripts/bench-resources.sh \
    "$GCE_NAME":~/ --zone="$GCE_ZONE" --quiet --scp-flag=-oStrictHostKeyChecking=no || die "scp failed"

  local rawfile="tmp/kv-scale-${GCE_MACHINE}.txt"; mkdir -p tmp
  log "=== running --measure-scale on $GCE_MACHINE (build + throughput + memory; the long part) -> $rawfile ==="
  "${GSSH[@]}" --command="
    source \$HOME/.cargo/env
    GCE_MACHINE='$GCE_MACHINE' MOON_REF='$MOON_REF' REDIS_VER='$REDIS_VER' \
    REQUESTS='$REQUESTS' KEYSPACE='$KEYSPACE' SHARDS='$SHARDS' \
      bash ~/gcloud-kv-scale-bench.sh --measure-scale
  " | tee "$rawfile"
  log "=== $GCE_MACHINE scale results in $rawfile; teardown follows (trap) ==="
}

gcloud_sweep(){
  log "=== KV-SCALE SWEEP: $MACHINES ==="
  local m short
  for m in $MACHINES; do
    short="${m%%-*}"
    log ""; log "##################### machine: $m #####################"
    ( GCE_MACHINE="$m"; GCE_NAME="moon-kv-scale-${short}"; gcloud_run_one ) \
      || log "WARN: machine $m run failed (see log); continuing"
  done
  log "=== sweep complete; per-machine raw files: tmp/kv-scale-<machine>.txt ==="
}

# ============================================================================= dispatch
case "${1:---gcloud}" in
  --self-test)     self_test ;;
  --measure-scale) measure_scale ;;
  --gcloud)        gcloud_sweep ;;
  --one)           gcloud_run_one ;;
  *) die "unknown subcommand: $1 (use --self-test | --measure-scale | --gcloud | --one)" ;;
esac
