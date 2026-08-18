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
#   --quick : host lint gates only (fmt, unsafe/unwrap audits, clippy x2).
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

cd "$REPO" || exit 2

tree_fingerprint() {
  # HEAD + a hash of the porcelain status (tracked mtimes don't matter;
  # content and branch do).
  echo "$(git rev-parse HEAD) $(git status --porcelain | git hash-object --stdin)"
}
FP_START="$(tree_fingerprint)"

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
