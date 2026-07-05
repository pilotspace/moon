#!/usr/bin/env bash
# gcloud-vector-soak.sh — vector-search reliability/stability/durability validation on GCloud, cross-arch.
#
# Runs scripts/vector-validate.py (recall/QPS vs baseline + long soak + kill-9 durability) on
# fresh GCE instances, comparing the CURRENT (possibly unpushed) branch against the v0.5.1 tag.
# The branch is shipped via `git bundle` so nothing needs to be pushed to GitHub first.
#
# Pattern follows gcloud-kv-scale-bench.sh (provision / SSH-wait / toolchain / teardown trap),
# plus the standing gotchas: ELF-magic assert (Mach-O trap), kill -9 only (SIGTERM+SO_REUSEPORT
# hang), distinct binary basenames (moon-base / moon-branch) so pkill backstops can't cross-kill.
#
# Subcommands:
#   --self-test   local gates: refs exist, bundle round-trips, driver compiles (NO GCloud, NO cost)
#   --remote      INNER; assumes it runs ON a provisioned Linux instance
#   --one         OUTER for a single $GCE_MACHINE
#   --gcloud      (default) OUTER sweep over $MACHINES sequentially
set -uo pipefail

BRANCH="${BRANCH:-perf/vector-search-optimization}"
BASE_REF="${BASE_REF:-v0.5.1}"
SOAK_MINUTES="${SOAK_MINUTES:-20}"
N_VECTORS="${N_VECTORS:-20000}"
GCE_MACHINE="${GCE_MACHINE:-c4a-standard-8}"
GCE_NAME="${GCE_NAME:-moon-vec-soak}"
GCE_ZONE="${GCE_ZONE:-us-central1-a}"
MACHINES="${MACHINES:-c4a-standard-8 c3-standard-8}"

log(){ printf '%s\n' "$*" >&2; }
die(){ log "FATAL: $*"; exit 1; }

# ============================================================================= self-test (no GCloud)
self_test(){
  local fails=0 tmp
  _ok(){  printf '  ok   %s\n' "$1" >&2; }
  _bad(){ printf '  FAIL %s\n' "$1" >&2; fails=$((fails+1)); }
  log "=== gcloud-vector-soak --self-test (refs + bundle + driver; no GCloud) ==="

  /usr/bin/git rev-parse -q --verify "$BRANCH"   >/dev/null && _ok "branch $BRANCH exists"  || _bad "branch $BRANCH missing"
  /usr/bin/git rev-parse -q --verify "$BASE_REF" >/dev/null && _ok "base $BASE_REF exists"  || _bad "base $BASE_REF missing"

  python3 -m py_compile scripts/vector-validate.py 2>/dev/null \
    && _ok "vector-validate.py compiles" || _bad "vector-validate.py does not compile"

  # Bundle round-trip: both refs must be clonable from the bundle.
  tmp=$(mktemp -d)
  if /usr/bin/git bundle create "$tmp/m.bundle" "$BASE_REF" "$BRANCH" >/dev/null 2>&1 \
     && /usr/bin/git clone -q -b "${BRANCH##*/}" "$tmp/m.bundle" "$tmp/clone" 2>/dev/null \
     && /usr/bin/git -C "$tmp/clone" rev-parse -q --verify "$BASE_REF" >/dev/null; then
    _ok "bundle carries $BASE_REF + $BRANCH"
  else
    # Bundles register branch refs verbatim; retry with the full ref name.
    if /usr/bin/git clone -q -b "$BRANCH" "$tmp/m.bundle" "$tmp/clone2" 2>/dev/null \
       && /usr/bin/git -C "$tmp/clone2" rev-parse -q --verify "$BASE_REF" >/dev/null; then
      _ok "bundle carries $BASE_REF + $BRANCH (full ref name)"
    else
      _bad "bundle round-trip failed"
    fi
  fi
  rm -rf "$tmp"

  # Result-parse gate: the driver's pass/fail JSON must be machine-readable.
  echo '{"pass": true, "failures": []}' | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d["pass"] else 1)' \
    && _ok "result JSON parse" || _bad "result JSON parse"

  [[ "$fails" -eq 0 ]] && { log "=== self-test PASS ==="; return 0; }
  log "=== self-test FAIL ($fails) ==="; return 1
}

# ============================================================================= inner: on the instance
elf_assert(){
  local magic; magic=$(od -An -tx1 -N4 "$1" | tr -d ' ')
  [[ "$magic" == "7f454c46" ]] || die "$1 not ELF (magic=$magic) — stale Mach-O?"
}

remote_run(){
  source "$HOME/.cargo/env" 2>/dev/null || true
  command -v cargo >/dev/null || die "cargo missing"
  [[ -f "$HOME/moon.bundle" ]] || die "moon.bundle not shipped"

  # Stale-server guard (leaked busy-poller gotcha): nothing moon-like may be running.
  pkill -9 -f 'moon-base|moon-branch' 2>/dev/null || true

  local src="$HOME/moon-src"
  if [[ ! -d "$src/.git" ]]; then
    git clone -q -b "$BRANCH" "$HOME/moon.bundle" "$src" 2>/dev/null \
      || git clone -q -b "${BRANCH##*/}" "$HOME/moon.bundle" "$src" \
      || die "clone from bundle failed"
  fi
  cd "$src"
  log "=== source: branch $(git rev-parse --short HEAD) / base $(git rev-parse --short "$BASE_REF") ==="

  # Build BASELINE first, park the binary, then build the branch.
  if [[ ! -x "$HOME/moon-base" ]]; then
    git checkout -q "$BASE_REF" || die "checkout $BASE_REF"
    log "=== building baseline ($BASE_REF) ==="
    cargo build --release >/dev/null 2>&1 || die "baseline build failed"
    cp target/release/moon "$HOME/moon-base"
  fi
  git checkout -q "$BRANCH" 2>/dev/null || git checkout -q "${BRANCH##*/}" || die "checkout $BRANCH"
  if [[ ! -x "$HOME/moon-branch" ]]; then
    log "=== building branch ($BRANCH) ==="
    cargo build --release >/dev/null 2>&1 || die "branch build failed"
    cp target/release/moon "$HOME/moon-branch"
  fi
  elf_assert "$HOME/moon-base"
  elf_assert "$HOME/moon-branch"

  log "=== running vector-validate.py (recall + ${SOAK_MINUTES}m soak + durability) ==="
  python3 "$HOME/vector-validate.py" \
    --moon-bin "$HOME/moon-branch" --baseline-bin "$HOME/moon-base" \
    --soak-minutes "$SOAK_MINUTES" --n-vectors "$N_VECTORS" \
    --out "$HOME/results.json"
  local rc=$?
  pkill -9 -f 'moon-base|moon-branch' 2>/dev/null || true
  log "=== validate exited rc=$rc ==="
  return "$rc"
}

# ============================================================================= gcloud orchestration
arch_image_family(){ case "$1" in c4a-*|t2a-*|*arm*|*arm64*) echo ubuntu-2404-lts-arm64;; *) echo ubuntu-2404-lts-amd64;; esac; }
# c4a (Axion) rejects pd-ssd — hyperdisk-balanced is its only SSD-class boot disk.
boot_disk_type(){ case "$1" in c4a-*|c4-*) echo hyperdisk-balanced;; *) echo pd-ssd;; esac; }

gcloud_run_one(){
  command -v gcloud >/dev/null || die "gcloud CLI not found"
  local imgfam; imgfam=$(arch_image_family "$GCE_MACHINE")
  local GSSH=(gcloud compute ssh "$GCE_NAME" --zone="$GCE_ZONE" --quiet
        --ssh-flag=-oStrictHostKeyChecking=no --ssh-flag=-oConnectTimeout=15)

  mkdir -p tmp
  log "=== bundling $BASE_REF + $BRANCH ==="
  /usr/bin/git bundle create "tmp/moon-vec-$GCE_NAME.bundle" "$BASE_REF" "$BRANCH" || die "git bundle failed"

  log "=== provisioning $GCE_NAME ($GCE_MACHINE, $GCE_ZONE, $imgfam) ==="
  gcloud compute instances create "$GCE_NAME" \
    --machine-type="$GCE_MACHINE" --zone="$GCE_ZONE" \
    --image-family="$imgfam" --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB --boot-disk-type="$(boot_disk_type "$GCE_MACHINE")" --quiet \
    || die "instance create failed"

  # Pin the actual name into the teardown trap NOW (env-prefix reverts GCE_NAME at fire time).
  local _inst="$GCE_NAME" _zone="$GCE_ZONE"
  trap "log '=== tearing down $_inst ==='; gcloud compute instances delete '$_inst' --zone='$_zone' -q 2>/dev/null || true" EXIT INT TERM

  log "=== waiting for SSH ==="
  local tries=0
  until "${GSSH[@]}" --command='echo up' >/dev/null 2>&1; do
    tries=$((tries+1)); [[ $tries -ge 40 ]] && die "SSH never came up"; sleep 5
  done

  log "=== provisioning toolchain (rust + numpy) ==="
  "${GSSH[@]}" --command='
    set -e
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential pkg-config libssl-dev git curl ca-certificates python3-numpy
    command -v cargo >/dev/null || curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1
  ' || die "instance provisioning failed"

  log "=== pushing bundle + harness ==="
  gcloud compute scp "tmp/moon-vec-$GCE_NAME.bundle" scripts/vector-validate.py scripts/gcloud-vector-soak.sh \
    "$GCE_NAME":~/ --zone="$GCE_ZONE" --quiet --scp-flag=-oStrictHostKeyChecking=no || die "scp failed"
  "${GSSH[@]}" --command="mv ~/moon-vec-$GCE_NAME.bundle ~/moon.bundle" || die "bundle rename failed"

  local rawfile="tmp/vec-soak-${GCE_MACHINE}.log"
  log "=== running --remote on $GCE_MACHINE (build x2 + validate; the long part) -> $rawfile ==="
  "${GSSH[@]}" --command="
    BRANCH='$BRANCH' BASE_REF='$BASE_REF' SOAK_MINUTES='$SOAK_MINUTES' N_VECTORS='$N_VECTORS' \
      bash ~/gcloud-vector-soak.sh --remote
  " 2>&1 | tee "$rawfile"
  local rc=${PIPESTATUS[0]}

  log "=== fetching results.json ==="
  gcloud compute scp "$GCE_NAME":~/results.json "tmp/vec-soak-${GCE_MACHINE}.json" \
    --zone="$GCE_ZONE" --quiet --scp-flag=-oStrictHostKeyChecking=no \
    || log "WARN: results.json fetch failed (validate rc=$rc)"
  log "=== $GCE_MACHINE done (rc=$rc); results tmp/vec-soak-${GCE_MACHINE}.json; teardown follows (trap) ==="
  return "$rc"
}

gcloud_sweep(){
  log "=== VECTOR-SOAK SWEEP: $MACHINES ==="
  local m short overall=0
  for m in $MACHINES; do
    short="${m%%-*}"
    log ""; log "##################### machine: $m #####################"
    ( GCE_MACHINE="$m"; GCE_NAME="moon-vec-soak-${short}"; gcloud_run_one ) \
      || { overall=1; log "WARN: machine $m run failed (see log); continuing"; }
  done
  log "=== sweep complete; per-machine results: tmp/vec-soak-<machine>.json ==="
  return "$overall"
}

# ============================================================================= dispatch
case "${1:---gcloud}" in
  --self-test) self_test ;;
  --remote)    remote_run ;;
  --gcloud)    gcloud_sweep ;;
  --one)       gcloud_run_one ;;
  *) die "unknown subcommand: $1 (use --self-test | --remote | --gcloud | --one)" ;;
esac
