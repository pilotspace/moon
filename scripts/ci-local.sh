#!/usr/bin/env bash
# ci-local.sh — the local merge bar (2026-08 CI migration).
#
# GitHub Actions' PR gate is now hosted-only and fast (lint + msrv + tokio
# leg + memory gate). The heavyweight legs that used to serialize on the
# single self-hosted runner run HERE instead, before every push:
#
#   default : THE MERGE BAR (= --full since moon#732). Host lint gates, the
#             two full suites in the moon-dev VM — monoio (the SHIPPED
#             runtime, io_uring live) and tokio (the CI-parity leg) — then
#             the client-compat harness and the macOS host suite.
#   --quick : host lint gates only (fmt, unsafe/unwrap audits, clippy x3)
#             — no VM legs, so no disk pre-flight.
#   --fast  : the pre-#732 default — lint gates + the two VM suites, WITHOUT
#             client-compat and the macOS suite. For iterating mid-change.
#             Not the merge bar; the summary refuses to call it one.
#   --full  : same as the default. Kept so existing docs and habits work.
#   --native: everything on the macOS HOST, no VM at all — host lint
#             gates, BOTH full suites (monoio on kqueue + tokio), and the
#             client-compat harness against brew's redis-server. For when
#             the VM is unavailable, or to keep a long testing phase on
#             one machine. See "What --native does NOT cover" below.
#
# Windows cannot run locally — before merging, still dispatch the hosted
# matrix:  gh workflow run ci.yml --ref <branch>
#
# What --native does NOT cover, and why it is printed on every run rather
# than left for the reader to remember:
#   * io_uring. macOS runs monoio on kqueue. The whole point of the VM
#     monoio leg is that the SHIPPED Linux path uses io_uring, and no
#     amount of native testing exercises it.
#   * Linux-only code behind cfg(target_os = "linux") — O_DIRECT, the
#     spin governor's /proc sampling, connection migration.
#   * Windows, and the MSRV toolchain pin.
# A local gate that quietly implies full coverage is worse than no gate,
# so --native ends by naming these instead of printing a bare PASS.
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

# Hardcoded on purpose: /Users/tindang/workspaces/tind-repo/moon is an OLD
# second checkout, and a relative path has resolved there before. CI_LOCAL_REPO
# overrides it for callers that legitimately live elsewhere — the pre-flight
# test harness runs this script on a hosted runner, where the hardcoded path
# does not exist and the `cd` below would exit 2 before any gate ran, which is
# indistinguishable from a gate deciding to exit 2.
REPO="${CI_LOCAL_REPO:-/Volumes/Games/tindang-repo/moon}"
VM="moon-dev"
# The bare invocation is the MERGE BAR, and since moon#732 that means `full`.
# The pre-merge hosted matrix no longer runs macOS or client-compat — they were
# duplicating this script on a slower machine — so if they are not gates HERE
# they are not gates anywhere until after merge. client-compat is not a
# formality: it is what caught the v0.8.6 inline-GET ACL bypass. `--fast` is
# the old default, kept for iterating mid-change; it is NOT the merge bar and
# the summary says so.
MODE="full"
case "${1:-}" in
  --quick)  MODE="quick" ;;
  --fast)   MODE="fast" ;;
  --full)   MODE="full" ;;
  --native) MODE="native" ;;
  "")       ;;
  *) echo "usage: $0 [--quick|--fast|--full|--native]" >&2; exit 2 ;;
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
# Re-measured on moon-dev 2026-08-26 after moon#655 set `[profile.dev]
# debug = "line-tables-only"`. Same tree, same `cargo nextest run --no-run`,
# only the debug setting differing, cold dir each time:
#
#            debug=2 (before)   line-tables-only   delta
#   dir      41,993,138,176     15,958,941,696     -62.0%
#   258 exe  36,592,516,376     11,783,054,240     -67.8%
#   build    449s               279s               -37.9%
#
# The two constants that describe what a build WRITES move with that; the
# warm-headroom warn line does not, because it describes free space a running
# suite wants underneath it, which moon#655 did not change. Leaving the cold
# figure at 36G would have refused legs that now genuinely fit -- and a
# pre-flight that grounds healthy runs gets disabled, after which it guards
# nothing (the failure mode an earlier draft of the host guard already hit).
DISK_FLOOR_BYTES=$((8 * 1024 * 1024 * 1024))    # below this OrbStack itself wedges
DISK_COLD_NEED_BYTES=$((18 * 1024 * 1024 * 1024)) # cold leg measured 14.9G + headroom
DISK_WARM_WARN_BYTES=$((15 * 1024 * 1024 * 1024)) # warm leg: only headroom is needed
                                                  # (unchanged: moon#655 shrank what a
                                                  # build WRITES, not the headroom a
                                                  # running suite wants underneath it)
DISK_COLD_DIR_BYTES=$((3 * 1024 * 1024 * 1024))   # a smaller dir is a stub, not a cache
                                                  # (a full one is ~15G, not ~34G)

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

# ── Host-side pre-flight (moon#661) ───────────────────────────────────
# The VM's own number is not the answer. On 2026-08-22 the pre-flight
# printed a green light on precisely the failure it was written to catch:
#
#   ✓ disk pre-flight (moon-dev)
#     local-monoio   target  41G  free  18G  OK      <- the VM told the truth
#   ✗ VM monoio suite FAILED (rc=1)                  <- no error text at all
#
# The macOS volume holding the VM's disk image was at 4.1G of 460G. The
# image AUTO-EXPANDS -- the VM root grew 124G -> 147G across that single
# run -- so the legs the pre-flight clears are themselves eating host
# headroom while they run, and free space inside the VM never sees it.
#
# A wrinkle worth stating: an in-guest `rm` frees nothing on the host. The
# image only gives blocks back after `fstrim`, so "I deleted a target dir"
# is not a host remedy on its own.
HOST_VOLUME=/System/Volumes/Data                    # NOT `/`: see read_host_avail_bytes
HOST_FLOOR_BYTES=$((10 * 1024 * 1024 * 1024))       # macOS itself misbehaves below this
HOST_WARN_SLACK_BYTES=$((15 * 1024 * 1024 * 1024))  # cushion above the arithmetic minimum
# Measured 2026-08-23 on moon-dev: one WARM tokio leg (full nextest suite,
# 5143 tests) grew the image 90.2 -> 90.5 GB and moved host free space by
# less than a gigabyte. A warm leg is cheap; a COLD one is not, because it
# materializes a whole ~15G target dir that the image must back (it was ~34G
# before moon#655 cut the debug info). An earlier
# draft of this guard charged 12G per warm leg and refused to start a run on
# a host with 33G free -- on a machine where ci-local had passed an hour
# before. A pre-flight that grounds healthy runs gets disabled, and then it
# guards nothing.
DISK_WARM_GROWTH_BYTES=$((2 * 1024 * 1024 * 1024))

# vm_growth_bytes <leg1-target-bytes> <leg2-target-bytes>
# How much the run will write INSIDE the VM, which is how much the image
# will claim from the host. A cold leg materializes a whole target dir; a
# warm one still grows the image by its incremental build.
vm_growth_bytes() {
  local total=0 tgt
  for tgt in "$@"; do
    if [ "$tgt" -lt "$DISK_COLD_DIR_BYTES" ]; then
      total=$((total + DISK_COLD_NEED_BYTES))
    else
      total=$((total + DISK_WARM_GROWTH_BYTES))
    fi
  done
  echo "$total"
}

# host_disk_verdict <host-avail-bytes> <vm-growth-bytes>
# Echoes OK | WARN | FAIL, non-zero only for FAIL -- same contract as
# disk_verdict, so a caller may check either the text or the status.
host_disk_verdict() {
  local avail=$1 growth=$2
  if [ "$avail" -lt "$HOST_FLOOR_BYTES" ]; then echo FAIL; return 1; fi
  if [ "$avail" -lt "$((HOST_FLOOR_BYTES + growth))" ]; then echo FAIL; return 1; fi
  if [ "$avail" -lt "$((HOST_FLOOR_BYTES + growth + HOST_WARN_SLACK_BYTES))" ]; then
    echo WARN; return 0
  fi
  echo OK; return 0
}

# read_host_avail_bytes — free bytes on the volume backing the VM image,
# or nothing at all if it cannot be read.
#
# Two spellings that look right and are not:
#   df -PB1   -- a GNU flag. macOS df rejects -B and prints usage, so a
#                probe written that way reads EMPTY forever, and under the
#                "cannot measure -> step aside" policy that is a guard
#                which never runs. Hence `-Pk` and a x1024.
#   df /      -- `/` is the sealed system snapshot. It shares the APFS
#                container's free space, so Available happens to match,
#                but Capacity does not (24% vs 93% on the same machine).
#                Anything keyed on the percentage is wrong there.
read_host_avail_bytes() {
  [ -d "$HOST_VOLUME" ] || return 0
  local kb
  kb=$(df -Pk "$HOST_VOLUME" 2>/dev/null | awk 'NR==2 {print $4}')
  case "$kb" in
    ''|*[!0-9]*) return 0 ;;
  esac
  echo $((kb * 1024))
}

# disk_facts — one line each for host and VM. Printed when a leg fails,
# because "rc=1 and no output" is the signature of a full disk and reads
# identically to a test failure. Cheap enough to print unconditionally on
# failure; detecting "the leg printed nothing" would mean capturing every
# leg's output, and a captured gate is a gate whose exit code is easy to
# lose down a pipe.
disk_facts() {
  local h
  h=$(read_host_avail_bytes)
  if [ -n "$h" ]; then
    echo "    host $HOST_VOLUME: $(as_gb "$h")G free"
  else
    echo "    host $HOST_VOLUME: UNREADABLE"
  fi
  local v
  v=$(orb run -m "$VM" bash -c 'df -Pk / | awk "NR==2 {print \$4}"' 2>/dev/null)
  case "$v" in
    ''|*[!0-9]*) echo "    $VM /: UNREADABLE (OrbStack may be wedged)" ;;
    *)           echo "    $VM /: $(as_gb $((v * 1024)))G free" ;;
  esac
}

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
  # moon#661: the host side. The VM's verdict above can be OK while the
  # macOS volume backing its auto-expanding image has nothing left, and
  # that failure arrives as rc=1 with no error text.
  local host_avail growth hv
  host_avail=$(read_host_avail_bytes)
  growth=$(vm_growth_bytes $(echo "$probe" | sed -n 2p) $(echo "$probe" | sed -n 3p))
  if [ -z "$host_avail" ]; then
    # Loud, not silent: a host check that quietly never runs is the exact
    # shape of the bug this guard was added for.
    echo "  host           NOT CHECKED — could not read $HOST_VOLUME"
  else
    hv=$(host_disk_verdict "$host_avail" "$growth") || rc=1
    printf "  %-14s needs %3sG  free %3sG  %s\n" \
      "host" "$(as_gb "$growth")" "$(as_gb "$host_avail")" "$hv"
  fi

  if [ $rc -ne 0 ]; then
    echo ""
    echo "  ✗ NOT ENOUGH DISK — refusing to start."
    echo "    A run started here fails with no error text and can wedge OrbStack."
    echo "    Biggest consumers:"
    orb run -m "$VM" bash -c 'du -sh ~/ci-target/* 2>/dev/null | sort -rh | head -6' 2>/dev/null | sed "s/^/      /"
    echo "    Reclaim inside the VM (each dir rebuilds from scratch, ~15G and ~5min):"
    echo "      orb run -m $VM bash -c 'rm -rf ~/ci-target/local-tokio'"
    echo "      orb run -m $VM bash -c 'rm -rf ~/ci-target/local-monoio'"
    echo "    Then return those blocks TO THE HOST — an in-guest rm frees"
    echo "    nothing on the host until the image is trimmed:"
    echo "      orb run -m $VM sudo fstrim -av"
    echo "    Check the host number again afterwards: fstrim reports every"
    echo "    unallocated block it trims, which is NOT what the host gets"
    echo "    back. Measured 2026-08-23: 82.8 GiB reported trimmed returned"
    echo "    under 1G of host space, because most of it was already sparse."
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
# Host equivalents for --native. Same nextest-or-fallback shape as the VM
# pair above: `--profile ci` gives the retry policy, and without nextest a
# plain `cargo test --no-fail-fast` still runs everything (no retries, so a
# flake then needs isolating by hand rather than being absorbed).
HOST_TEST_MONOIO='if command -v cargo-nextest >/dev/null 2>&1; then
    cargo nextest run --profile ci
  else
    echo "(nextest absent — falling back to cargo test --no-fail-fast)"
    cargo test --no-fail-fast
  fi'
HOST_TEST_TOKIO='if command -v cargo-nextest >/dev/null 2>&1; then
    cargo nextest run --profile ci --no-default-features --features runtime-tokio,jemalloc
  else
    cargo test --no-default-features --features runtime-tokio,jemalloc --no-fail-fast
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
if [ "$MODE" != "quick" ] && [ "$MODE" != "native" ]; then
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
# The `console` feature compiles a module (src/admin/console_gateway.rs) that
# NOTHING else here builds: not the default clippy above, not the tokio one,
# and not either VM suite. On Actions it is checked only by `Check (console
# feature)`, which runs on main-push and workflow_dispatch — never on a PR. So
# a change to a shared type could pass every local gate AND the whole PR gate
# and still break the build, which is what moon#705 did (E0308 in the gateway's
# Execute-reply consumer, found only after the dispatch matrix ran). Seconds
# here, against a whole dispatch cycle there. No pnpm build is needed: without
# console/dist the rust_embed macro embeds nothing, which still type-checks.
run_step "clippy (console)"   env CARGO_TARGET_DIR=target-console \
  cargo clippy --no-default-features \
  --features runtime-monoio,jemalloc,graph,text-index,console -- -D warnings || exit 1

if [ "$MODE" != "quick" ] && [ "$MODE" != "native" ]; then
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

if [ "$MODE" = "native" ]; then
  # ── Native phase 1: both full suites on the macOS host ─────────────
  # Sequential and in separate target dirs: the two feature sets are not
  # link-compatible, so sharing one dir makes each leg relink the world.
  # The tokio leg reuses `target-tokio`, which the tokio clippy gate above
  # already warmed; the monoio leg uses the default `target/`, shared with
  # the release build below (the default clippy gate deliberately sits in
  # `target-clippy` so a `-D warnings` run never invalidates it).
  #
  # Default features ARE runtime-monoio, which on macOS drives kqueue
  # rather than io_uring. That is a real gap, not a substitution — it is
  # named in the summary rather than papered over.
  run_step "native monoio suite (kqueue, NOT io_uring)" \
    env MOON_DISK_FREE_MIN_PCT=0 bash -c "$HOST_TEST_MONOIO"
  run_step "native tokio suite" \
    env MOON_NO_URING=1 CARGO_TARGET_DIR=target-tokio MOON_DISK_FREE_MIN_PCT=0 \
    bash -c "$HOST_TEST_TOKIO"

  # ── Native phase 2: client-compat against brew's redis-server ──────
  # Refuses rather than skips when the oracle is missing: a compat gate
  # that silently does not run is the exact "green because it never ran"
  # shape this file exists to prevent.
  if ! command -v redis-server >/dev/null 2>&1; then
    echo "  ✗ client-compat needs a real redis-server oracle and none is on PATH."
    echo "    brew install redis   (the harness diffs Moon against it byte for byte)"
    exit 2
  fi
  run_step "native build release (for compat)" cargo build --release
  # MOON_BIN is pinned to the binary built THIS run, and passed as an
  # explicit flag rather than the env var: find_moon_binary()'s fallback
  # has graded a stale quarantined binary before now.
  run_step "native client-compat (strict + contexts)" \
    env MOON_DISK_FREE_MIN_PCT=0 ./scripts/test-client-compat.sh --strict \
      --contexts standalone,multi,pipeline \
      --moon-bin "$REPO/target/release/moon"
  run_step "native client-compat (INFO coverage)" \
    env MOON_DISK_FREE_MIN_PCT=0 ./scripts/test-client-compat.sh \
      --filter __none__ --info-manifest \
      --moon-bin "$REPO/target/release/moon"
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
  # moon#661: read the disk BEFORE reading the test list. A leg that failed
  # because nothing could be written exits rc=1 with no error text, which
  # is indistinguishable from a test failure until you look here.
  echo "  disk at end of run:"
  disk_facts
  echo "  RESULT: FAIL — re-run failing suites in isolation before"
  echo "  attributing (known load-flake classes: fixed ports, kill-9 timing)."
  exit 1
fi
if [ "$MODE" = "native" ]; then
  # A pass here is a pass on THIS platform. Spelling out the gap is the
  # whole reason the mode is allowed to exist: the failure it guards
  # against is someone reading "RESULT: PASS" as "ready to merge".
  echo "  RESULT: PASS (native/macOS) — this did NOT cover:"
  echo "    · io_uring   — monoio ran on kqueue here; the shipped Linux"
  echo "                   path is io_uring and is untested by this run"
  echo "    · Linux-only cfg code (O_DIRECT, spin governor, migration)"
  echo "    · Windows, and the MSRV 1.94 toolchain pin"
  echo "  Dispatch the hosted matrix before merging:"
  echo "    gh workflow run ci.yml --ref \$(git branch --show-current)"
  exit 0
fi
# Only the merge bar is allowed to read like the merge bar. Before moon#732
# every mode printed this same line, so `--quick` — six lint gates and not one
# test — announced "RESULT: PASS" exactly as a full run did. A verdict that
# cannot distinguish what it ran from what it skipped is how a partial gate
# gets mistaken for a complete one.
case "$MODE" in
  quick)
    echo "  RESULT: PASS (quick) — LINT ONLY. NOT the merge bar."
    echo "    No tests ran: no VM suites, no client-compat, no macOS suite."
    echo "    Run \`scripts/ci-local.sh\` with no arguments before pushing."
    exit 0 ;;
  fast)
    echo "  RESULT: PASS (fast) — NOT the merge bar."
    echo "    Skipped: client-compat (the harness that caught the v0.8.6"
    echo "    inline-GET ACL bypass) and the macOS host suite. Since moon#732"
    echo "    the hosted matrix no longer runs either, so nothing has gated"
    echo "    them. Run \`scripts/ci-local.sh\` with no arguments before pushing."
    exit 0 ;;
esac
echo "  RESULT: PASS — merge bar satisfied except Windows, which cannot run"
echo "  locally. Dispatch the hosted matrix (Windows + MSRV + memory gate):"
echo "    gh workflow run ci.yml --ref \$(git branch --show-current)"
exit 0
