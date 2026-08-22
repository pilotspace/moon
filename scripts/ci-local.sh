#!/usr/bin/env bash
# ci-local.sh — the local merge bar (2026-08 CI migration).
#
# GitHub Actions' PR gate is now hosted-only and fast (lint + msrv + tokio
# leg + memory gate). The heavyweight legs that used to serialize on the
# single self-hosted runner run HERE instead, before every push:
#
#   default : host lint gates, then the two full test suites in the
#             moon-dev VM — monoio (the SHIPPED runtime, io_uring live)
#             and tokio (the CI-parity leg).
#   --quick : host lint gates only (fmt, unsafe/unwrap audits, clippy x2)
#             — no VM legs, so no disk pre-flight.
#   --full  : default + client-compat harness (VM, real redis-server
#             oracle) + macOS host test suite.
#
# Windows cannot run locally — before merging, still dispatch the hosted
# matrix:  gh workflow run ci.yml --ref <branch>
#
# Design notes (each guards against a failure this repo has actually hit):
#   * Every step's exit code is captured directly — never through a pipe
#     (a `cmd | tail && echo OK` gate once printed OK over a failure).
#   * VM builds use VM-LOCAL target dirs under ~/ci-target — never the
#     shared /Volumes/Games volume (hovers at the diskfull guard) and
#     never /tmp (tmpfs fills with target dirs).
#   * client-compat pins MOON_BIN to the binary built THIS run — the
#     harness's find_moon_binary() fallback happily grades a days-old
#     quarantined binary otherwise.
#   * A disk pre-flight runs before anything else: a full VM root makes
#     legs fail with no error text and can wedge OrbStack outright, a
#     signature that reads as test flakiness for hours.
#   * The tree is fingerprinted at start and re-checked at the end: a
#     branch switch or edit mid-run silently invalidates every result
#     (suites compile from a mutating tree), so the run refuses to
#     report green if the tree changed.

set -u -o pipefail

REPO="/Volumes/Games/tindang-repo/moon"
VM="moon-dev"
MODE="default"
case "${1:-}" in
  --quick) MODE="quick" ;;
  --full)  MODE="full" ;;
  "")      ;;
  *) echo "usage: $0 [--quick|--full]" >&2; exit 2 ;;
esac

# Library mode is sourced from anywhere — CI runs the pre-flight test on a
# hosted runner where $REPO does not exist, and an unconditional `cd` here
# exits the sourcing shell before a single function is defined.
if [ -z "${CI_LOCAL_LIB_ONLY:-}" ]; then
  cd "$REPO" || exit 2
fi

tree_fingerprint() {
  # HEAD + a hash of the porcelain status (tracked mtimes don't matter;
  # content and branch do).
  echo "$(git rev-parse HEAD) $(git status --porcelain | git hash-object --stdin)"
}
# Not in library mode: the fingerprint shells out to git, and the test
# harness may source this from a directory that is not a checkout.
if [ -z "${CI_LOCAL_LIB_ONLY:-}" ]; then
  FP_START="$(tree_fingerprint)"
fi

declare -a NAMES RCS SECS
run_step() { # run_step <name> <cmd...>
  local name=$1; shift
  local t0 t1 rc
  echo ""
  echo "━━━ ${name} ━━━"
  t0=$(date +%s)
  "$@"
  rc=$?
  t1=$(date +%s)
  NAMES+=("$name"); RCS+=($rc); SECS+=($((t1 - t0)))
  if [ $rc -ne 0 ]; then
    echo "✗ ${name} FAILED (rc=$rc)"
  else
    echo "✓ ${name} ok ($((t1 - t0))s)"
  fi
  return $rc
}

vm() { # vm <shell-command> — run inside the moon-dev VM at the repo
  orb run -m "$VM" bash -c "source ~/.cargo/env && cd $REPO && $*"
}

# ── Disk pre-flight (moon#658) ────────────────────────────────────────
# A full VM root does not announce itself. On 2026-08-22 the tokio leg
# exited rc=1 printing nothing, and the next `orb run` answered "sconrpc
# ready event fired but socket was not connectible" — OrbStack had wedged
# on a 97%-full root. The visible symptom was three unrelated-looking test
# flakes; the cause was 4G of free space.
#
# Sizes measured on moon-dev 2026-08-22: root 124G, and each of the two
# VM leg target dirs (~/ci-target/local-{monoio,tokio}) is 34G warm.
DISK_FLOOR_BYTES=$((8 * 1024 * 1024 * 1024))    # below this OrbStack itself wedges
DISK_COLD_NEED_BYTES=$((36 * 1024 * 1024 * 1024)) # a leg with no target dir builds ~34G
DISK_WARM_WARN_BYTES=$((15 * 1024 * 1024 * 1024)) # warm leg: only headroom is needed
DISK_COLD_DIR_BYTES=$((5 * 1024 * 1024 * 1024))   # a smaller dir is a stub, not a cache

# disk_verdict <avail-bytes> <leg-target-dir-bytes>
# Echoes OK | WARN | FAIL and returns non-zero only for FAIL, so a caller
# that checks either the text or the status reaches the same conclusion.
disk_verdict() {
  local avail=$1 tgt=$2
  if [ "$avail" -lt "$DISK_FLOOR_BYTES" ]; then
    echo FAIL; return 1
  fi
  if [ "$tgt" -lt "$DISK_COLD_DIR_BYTES" ]; then
    # Cold: this leg has to materialize a whole target dir.
    if [ "$avail" -lt "$DISK_COLD_NEED_BYTES" ]; then echo FAIL; return 1; fi
    echo OK; return 0
  fi
  if [ "$avail" -lt "$DISK_WARM_WARN_BYTES" ]; then echo WARN; return 0; fi
  echo OK; return 0
}

as_gb() { echo $(( $1 / 1024 / 1024 / 1024 )); }

# preflight_disk — read the VM's real numbers, judge each leg in the order
# it runs, and refuse to start a run that cannot finish. Failure to READ
# the numbers is never itself a blocker: a pre-flight that can't measure
# steps aside rather than grounding a healthy run.
preflight_disk() {
  local probe avail sizes rc=0
  probe=$(orb run -m "$VM" bash -c '
      df -PB1 / | awk "NR==2 {print \$4}"
      for d in local-monoio local-tokio; do
        if [ -d "$HOME/ci-target/$d" ]; then
          du -sB1 "$HOME/ci-target/$d" 2>/dev/null | awk "{print \$1}"
        else
          echo 0
        fi
      done' 2>/dev/null)
  if [ "$(echo "$probe" | wc -l | tr -d " ")" -ne 3 ]; then
    echo "  (could not read VM free space — pre-flight skipped, not blocking)"
    return 0
  fi
  avail=$(echo "$probe" | sed -n 1p)
  local projected=$avail i=2
  for leg in local-monoio local-tokio; do
    local tgt v
    tgt=$(echo "$probe" | sed -n "${i}p"); i=$((i + 1))
    v=$(disk_verdict "$projected" "$tgt") || rc=1
    printf "  %-14s target %3sG  free %3sG  %s\n" "$leg" "$(as_gb "$tgt")" "$(as_gb "$projected")" "$v"
    # A cold leg will consume its build before the next leg starts, so the
    # next leg is judged against what will actually be left.
    if [ "$tgt" -lt "$DISK_COLD_DIR_BYTES" ]; then
      projected=$((projected - DISK_COLD_NEED_BYTES))
      [ "$projected" -lt 0 ] && projected=0
    fi
  done
  if [ $rc -ne 0 ]; then
    echo ""
    echo "  ✗ NOT ENOUGH DISK IN $VM — refusing to start."
    echo "    A run started here fails with no error text and can wedge OrbStack."
    echo "    Biggest consumers:"
    orb run -m "$VM" bash -c 'du -sh ~/ci-target/* 2>/dev/null | sort -rh | head -6' 2>/dev/null | sed "s/^/      /"
    echo "    Reclaim (each dir rebuilds from scratch, ~34G and ~25min):"
    echo "      orb run -m $VM bash -c 'rm -rf ~/ci-target/local-tokio'"
    echo "      orb run -m $VM bash -c 'rm -rf ~/ci-target/local-monoio'"
  fi
  return $rc
}

# nextest gives the CI retry profile; fall back to plain cargo test when
# absent (slower, no retries — flakes then need manual isolation re-runs).
VM_TEST_MONOIO='if command -v cargo-nextest >/dev/null 2>&1; then
    cargo nextest run --profile ci
  else
    echo "(nextest absent in VM — falling back to cargo test --no-fail-fast)"
    cargo test --no-fail-fast
  fi'
VM_TEST_TOKIO='if command -v cargo-nextest >/dev/null 2>&1; then
    cargo nextest run --profile ci --no-default-features --features runtime-tokio,jemalloc
  else
    cargo test --no-default-features --features runtime-tokio,jemalloc --no-fail-fast
  fi'

# ── Library mode ──────────────────────────────────────────────────────
# `CI_LOCAL_LIB_ONLY=1 . scripts/ci-local.sh` loads the functions above and
# runs no gates, so scripts/test-ci-local-preflight.sh can exercise the
# pre-flight decision without a VM. Without this, sourcing the script to
# test one function starts a full two-suite run.
if [ -n "${CI_LOCAL_LIB_ONLY:-}" ]; then
  return 0 2>/dev/null || exit 0
fi

# The VM legs are the ones that need room; --quick is host-only. The host
# repo volume is not checked: it carries 3G target dirs, not 34G ones.
if [ "$MODE" != "quick" ]; then
  run_step "disk pre-flight ($VM)" preflight_disk || exit 1
fi

# ── Phase 0: host lint gates (fail fast — nothing else is worth running
# on top of a fmt/clippy failure) ─────────────────────────────────────
run_step "fmt --check"        cargo fmt --check                        || exit 1
run_step "audit-unsafe"       bash scripts/audit-unsafe.sh             || exit 1
run_step "audit-unwrap"       bash scripts/audit-unwrap.sh             || exit 1
run_step "clippy (default)"   env CARGO_TARGET_DIR=target-clippy \
  cargo clippy -- -D warnings                                          || exit 1
run_step "clippy (tokio)"     env CARGO_TARGET_DIR=target-tokio \
  cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings || exit 1

if [ "$MODE" != "quick" ]; then
  # ── Phase 1: the two full suites, in the Linux VM ───────────────────
  # Sequential, monoio (shipped runtime) first: parallel VM builds of two
  # feature sets contend on memory and the shared-volume virtiofs.
  # `export VAR=...;` (not a bare env prefix): $VM_TEST_* expands to an
  # `if` COMPOUND command, and `VAR=x if ...` is a bash syntax error —
  # phase 1 exited in <1s with rc=2 without running a single test the
  # first time --full was exercised (2026-08-19).
  run_step "VM monoio suite (shipped runtime)" \
    vm "export CARGO_TARGET_DIR=\$HOME/ci-target/local-monoio MOON_DISK_FREE_MIN_PCT=0; $VM_TEST_MONOIO"
  run_step "VM tokio suite" \
    vm "export CARGO_TARGET_DIR=\$HOME/ci-target/local-tokio MOON_NO_URING=1 MOON_DISK_FREE_MIN_PCT=0; $VM_TEST_TOKIO"
fi

if [ "$MODE" = "full" ]; then
  # ── Phase 2: client-compat (VM — it has the redis-server oracle) ────
  run_step "VM build release (monoio) for compat" \
    vm "CARGO_TARGET_DIR=\$HOME/ci-target/local-compat cargo build --release"
  run_step "VM client-compat (strict + contexts)" \
    vm "MOON_BIN=\$HOME/ci-target/local-compat/release/moon MOON_NO_URING=1 MOON_DISK_FREE_MIN_PCT=0 \
        ./scripts/test-client-compat.sh --strict --contexts standalone,multi,pipeline"
  # ── Phase 3: macOS host suite (tokio — kqueue) ──────────────────────
  run_step "macOS host tokio suite" \
    env MOON_NO_URING=1 CARGO_TARGET_DIR=target-tokio \
    cargo test --no-default-features --features runtime-tokio,jemalloc --no-fail-fast
fi

# ── Tree-mutation tripwire ────────────────────────────────────────────
FP_END="$(tree_fingerprint)"
TREE_OK=0
if [ "$FP_START" != "$FP_END" ]; then
  TREE_OK=1
fi

# ── Summary ───────────────────────────────────────────────────────────
echo ""
echo "══════════════ ci-local summary (${MODE}) ══════════════"
FAILED=0
for i in "${!NAMES[@]}"; do
  if [ "${RCS[$i]}" -eq 0 ]; then s="PASS"; else s="FAIL"; FAILED=1; fi
  printf "  %-42s %s  %4ss\n" "${NAMES[$i]}" "$s" "${SECS[$i]}"
done
if [ $TREE_OK -ne 0 ]; then
  echo "  ✗ TREE CHANGED DURING THE RUN — results are INVALID."
  echo "    (branch switch or edit mid-run; suites compiled a mutating tree)"
  exit 3
fi
if [ $FAILED -ne 0 ]; then
  echo "  RESULT: FAIL — re-run failing suites in isolation before"
  echo "  attributing (known load-flake classes: fixed ports, kill-9 timing)."
  exit 1
fi
echo "  RESULT: PASS — remember: Windows still needs the hosted matrix:"
echo "    gh workflow run ci.yml --ref \$(git branch --show-current)"
exit 0
