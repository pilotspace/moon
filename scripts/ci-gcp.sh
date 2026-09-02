#!/usr/bin/env bash
# ci-gcp.sh — run the Linux legs of the merge bar on a real GCE host.
#
# WHY THIS EXISTS
#   scripts/ci-local.sh drives its Linux legs through the moon-dev OrbStack VM.
#   That VM has now vanished five times; each time ci-local.sh exits 4 and the
#   only local fallback is `--native`, which runs on macOS and therefore CANNOT
#   exercise io_uring, Linux-only `cfg` code, or the MSRV pin. This script runs
#   the same legs on an x86_64 Linux host in GCP instead.
#
#   It is strictly BETTER than the OrbStack VM for one specific reason: moon
#   ships on Linux with io_uring, and the OrbStack VM runs on Apple Silicon
#   under a kernel where the io_uring paths are not what production runs. This
#   host is x86_64 with a stock GCE kernel, so the monoio leg here drives the
#   io_uring driver LIVE — the single largest gap in `--native`.
#
# WHAT IT COVERS
#   1. monoio suite, default features, io_uring LIVE  (the shipped runtime)
#   2. tokio suite, --no-default-features runtime-tokio,jemalloc, MOON_NO_URING=1
#   3. client-compat against a real redis-server oracle
#   4. MSRV pin (1.94.0) — `cargo check`, matching the hosted job
#
# WHAT IT DOES NOT COVER
#   Windows. Nothing local does; dispatch the hosted matrix for that.
#   The script says so at the end rather than printing a bare PASS.
#
# EXIT CODES (mirroring ci-local.sh's contract, so callers can be shared)
#   0  all legs passed
#   1  a leg failed
#   2  preconditions missing on the remote (no cargo / no redis-server)
#   3  the remote checkout is not the commit we asked for  (INVALID, not a pass)
#   4  the remote is unreachable, or too little disk to run  (refused up front)
set -uo pipefail

HOST="${CI_GCP_HOST:-moon-bench-x86}"
ZONE="${CI_GCP_ZONE:-us-central1-a}"
REPO_REMOTE="${CI_GCP_REPO:-\$HOME/moon}"
# Measured, not guessed: with CARGO_PROFILE_DEV_DEBUG=0 the four target dirs
# total ~7.3G (monoio 2.8, tokio 2.9, release 0.9, msrv 0.7). 12G leaves room
# for a cold rebuild of the largest plus the release binary. An earlier 18G
# threshold was set from an assumption about DWARF-laden dev builds that the
# debug=0 setting had already made false, and it refused runs the host could
# comfortably have served.
MIN_FREE_GB="${CI_GCP_MIN_FREE_GB:-12}"

say() { printf '\n\033[1m━━━ %s ━━━\033[0m\n' "$*"; }
ok()  { printf '  \033[32m✓\033[0m %s\n' "$*"; }
bad() { printf '  \033[31m✗\033[0m %s\n' "$*"; }

remote() { gcloud compute ssh "$HOST" --zone "$ZONE" --command "$1" 2>&1 | grep -vE '^Warning: Permanently added|passphrase'; }

# ── Which commit are we gating? ───────────────────────────────────────────
SHA="$(git rev-parse HEAD)"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if ! git diff --quiet || ! git diff --cached --quiet; then
  bad "working tree is dirty — commit first, or the remote gates a DIFFERENT tree than you are looking at"
  exit 3
fi
echo "gating $BRANCH @ ${SHA:0:10} on $HOST ($ZONE)"

# ── Pre-flight: reachable, tooled, and enough disk ────────────────────────
say "pre-flight"
PRE="$(remote "
  export PATH=\$HOME/.cargo/bin:\$PATH
  echo ARCH=\$(uname -m)
  echo KERNEL=\$(uname -r)
  echo CARGO=\$(command -v cargo || echo MISSING)
  echo REDIS=\$(command -v redis-server || echo MISSING)
  echo MSRV=\$(ls \$HOME/.rustup/toolchains 2>/dev/null | grep -c 1.94.0)
  echo FREEGB=\$(df --output=avail -BG / | tail -1 | tr -dc '0-9')
  echo URING=\$(grep -c io_uring /proc/kallsyms 2>/dev/null || echo 0)
")" || true
if [ -z "$PRE" ] || ! grep -q '^ARCH=' <<<"$PRE"; then
  bad "cannot reach $HOST in $ZONE — nothing was run"
  echo "    gcloud compute instances list  # is it up?"
  exit 4
fi
eval "$(grep -E '^(ARCH|KERNEL|CARGO|REDIS|MSRV|FREEGB|URING)=' <<<"$PRE")"
echo "  arch=$ARCH kernel=$KERNEL free=${FREEGB}G io_uring_syms=$URING"
[ "$CARGO" = MISSING ] && { bad "no cargo on $HOST"; exit 2; }
[ "$REDIS" = MISSING ] && { bad "no redis-server on $HOST — the compat leg has no oracle"; exit 2; }
[ "$ARCH" = x86_64 ] || { bad "expected x86_64, got $ARCH"; exit 2; }
[ "${URING:-0}" -lt 10 ] && { bad "kernel reports no io_uring symbols — the point of this host is gone"; exit 2; }
if [ "${FREEGB:-0}" -lt "$MIN_FREE_GB" ]; then
  bad "only ${FREEGB}G free, need ${MIN_FREE_GB}G — refusing before a build wedges the host"
  echo "    on the host:  rm -rf ~/ci-target/*  ~/moon/target"
  exit 4
fi
ok "pre-flight ok"

# ── Ship the exact commit, and PROVE the remote is on it ──────────────────
say "ship $BRANCH @ ${SHA:0:10}"
BUNDLE="$(mktemp -t moon-gate.XXXXXX).bundle"
trap 'rm -f "$BUNDLE"' EXIT
# `main..HEAD` would send nothing when gating main itself.
git bundle create "$BUNDLE" HEAD --not --remotes=origin 2>/dev/null \
  || git bundle create "$BUNDLE" HEAD~50..HEAD 2>/dev/null \
  || git bundle create "$BUNDLE" HEAD
gcloud compute scp "$BUNDLE" "$HOST:/tmp/moon-gate.bundle" --zone "$ZONE" >/dev/null 2>&1 || {
  bad "scp of the bundle failed"; exit 4; }

GOT="$(remote "
  cd $REPO_REMOTE || exit 9
  git fetch /tmp/moon-gate.bundle '+HEAD:refs/heads/cigate' -f >/dev/null 2>&1
  git checkout -q --detach cigate 2>/dev/null
  # Self-maintenance: every run fetches a bundle and those objects accumulate.
  # Measured: .git reached 3.3G over a single session, pushed free space under
  # the pre-flight threshold, and the gate began refusing its OWN runs. A gc
  # once .git passes 2G brought it back to 357M in seconds.
  if [ \"\$(du -sm .git 2>/dev/null | cut -f1)\" -gt 2048 ]; then
    git branch -D cigate >/dev/null 2>&1
    git reflog expire --expire=now --all >/dev/null 2>&1
    git gc --prune=now --quiet >/dev/null 2>&1
  fi
  git rev-parse HEAD
" | tr -d '[:space:]')"
if [ "$GOT" != "$SHA" ]; then
  bad "remote is on ${GOT:0:10}, expected ${SHA:0:10} — results would describe a different tree"
  exit 3
fi
ok "remote checked out ${SHA:0:10}"

# ── The legs ──────────────────────────────────────────────────────────────
FAILED=0
leg() { # leg <name> <remote-shell-command>
  local name="$1" cmd="$2" start rc
  say "$name"
  start=$SECONDS
  # Capture rc DIRECTLY — never through a pipe, which would report the
  # exit status of `tail` and turn a red leg green.
  remote "$cmd" > "/tmp/cigcp-$$.log" 2>&1
  rc=$?
  if [ $rc -eq 0 ] && ! grep -qE '^(error|test result: FAILED)' "/tmp/cigcp-$$.log"; then
    ok "$name ok ($((SECONDS-start))s)"
  else
    bad "$name FAILED rc=$rc ($((SECONDS-start))s)"
    tail -40 "/tmp/cigcp-$$.log"
    FAILED=1
  fi
  rm -f "/tmp/cigcp-$$.log"
}

# MOON_DISK_FREE_MIN_PCT=0: the suite spawns servers that refuse to start when
# the disk guard trips, which on a shared CI host is a false failure, not a bug.
#
# Profile: the DEV profile, matching what ci-local.sh's VM legs run — a
# `--release` leg would exercise differently-optimised code than the merge bar
# does, and fat LTO would dominate the wall clock.
#
# CARGO_PROFILE_DEV_DEBUG=0 is the difference between this fitting on the host
# and not: a dev-profile target dir for this project measured ~39G per leg,
# **96% of it DWARF**, and two legs plus a release build do not fit in 48G.
# Dropping debuginfo changes no test outcome; it only makes a backtrace less
# readable, and a failing leg is re-run locally anyway.
ENVBASE="export PATH=\$HOME/.cargo/bin:\$PATH MOON_DISK_FREE_MIN_PCT=0 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0; cd $REPO_REMOTE;"

# 1. THE leg --native cannot produce: monoio with io_uring live.
leg "monoio suite (io_uring LIVE — the shipped runtime)" \
  "$ENVBASE export CARGO_TARGET_DIR=\$HOME/ci-target/gcp-monoio; cargo test --no-fail-fast 2>&1 | tail -60"

leg "tokio suite (MOON_NO_URING=1)" \
  "$ENVBASE export CARGO_TARGET_DIR=\$HOME/ci-target/gcp-tokio MOON_NO_URING=1; cargo test --no-default-features --features runtime-tokio,jemalloc --no-fail-fast 2>&1 | tail -60"

# 3. client-compat needs a release binary; pin MOON_BIN to THIS build, never
#    let find_moon_binary() fall back to a target/release/moon of unknown
#    provenance (that has produced a green run against a stale binary before).
leg "client-compat vs real redis-server" \
  "$ENVBASE export CARGO_TARGET_DIR=\$HOME/ci-target/gcp-rel;
   cargo build --release 2>&1 | tail -5;
   export MOON_BIN=\$HOME/ci-target/gcp-rel/release/moon;
   test -x \$MOON_BIN || { echo 'error: MOON_BIN missing'; exit 1; };
   ./scripts/test-client-compat.sh 2>&1 | tail -40"

leg "MSRV 1.94 (cargo check)" \
  "$ENVBASE export CARGO_TARGET_DIR=\$HOME/ci-target/gcp-msrv; cargo +1.94.0 check --all-targets 2>&1 | tail -20"

# ── Verdict, naming the gap rather than printing a bare PASS ──────────────
say "verdict"
if [ $FAILED -eq 0 ]; then
  ok "all GCP Linux legs passed for ${SHA:0:10}"
else
  bad "at least one leg failed for ${SHA:0:10}"
fi
cat <<EOF

  Covered here that --native CANNOT cover:
    - io_uring driver, live, on an x86_64 kernel
    - Linux-only cfg(target_os = "linux") code paths
    - the MSRV 1.94 pin
    - client-compat against a real redis-server oracle

  STILL NOT covered by any local gate:
    - Windows  ->  gh workflow run ci.yml --ref $BRANCH
    - memory steady-state (hosted)  ->  same dispatch

EOF
exit $FAILED
