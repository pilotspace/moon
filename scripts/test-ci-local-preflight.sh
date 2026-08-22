#!/usr/bin/env bash
# test-ci-local-preflight.sh — red/green tests for ci-local.sh's disk
# pre-flight decision (moon#658).
#
# The pre-flight exists because a full VM root is INVISIBLE in the failure
# it causes: on 2026-08-22 the tokio leg exited rc=1 with no error text,
# and the next `orb run` answered "sconrpc ready event fired but socket
# was not connectible" — OrbStack had wedged on a 97%-full VM root. Nothing
# in that signature says "disk"; hours went to reading test flakes instead.
#
# `disk_verdict` is a pure function so it can be tested without filling a
# disk. Sourcing ci-local.sh with CI_LOCAL_LIB_ONLY=1 loads the functions
# and runs no gates.

set -u -o pipefail
cd "$(dirname "$0")/.." || exit 2

CI_LOCAL_LIB_ONLY=1 . scripts/ci-local.sh || { echo "FATAL: cannot source ci-local.sh as a library"; exit 2; }

G=$((1024 * 1024 * 1024))
PASS=0; FAIL=0

expect() { # expect <label> <want-verdict> <avail-gb> <target-gb>
  local label=$1 want=$2 avail=$3 tgt=$4 got rc
  got=$(disk_verdict $((avail * G)) $((tgt * G)))
  rc=$?
  # A FAIL verdict must also be reported through the exit code — a
  # pre-flight that prints FAIL and returns 0 is a pre-flight that never
  # stops anything (moon: "a guard must be able to report its own failure").
  if [ "$got" = "$want" ] && { [ "$want" != "FAIL" ] || [ $rc -ne 0 ]; } \
     && { [ "$want" = "FAIL" ] || [ $rc -eq 0 ]; }; then
    PASS=$((PASS + 1)); printf "  ok    %-46s %s\n" "$label" "$got"
  else
    FAIL=$((FAIL + 1)); printf "  FAIL  %-46s got=%s(rc=%s) want=%s\n" "$label" "$got" "$rc" "$want"
  fi
}

echo "── disk_verdict ──"
# The wedge itself: below the floor nothing may start, warm dir or not.
expect "3G free, warm 34G dir  (the 2026-08-22 wedge)" FAIL  3 34
expect "7G free, warm 34G dir"                         FAIL  7 34
# Cold build needs room for a whole ~34G target dir.
expect "20G free, no target dir (cold build)"          FAIL 20  0
expect "40G free, no target dir (cold build)"          OK   40  0
# Warm incremental: 34G is already spent, only headroom is needed.
expect "24G free, warm 34G dir (today's VM)"           OK   24 34
expect "10G free, warm 34G dir (thin but usable)"      WARN 10 34
# A dir that exists but is a stub is still a cold build.
expect "20G free, 2G stub dir"                         FAIL 20  2

echo ""
echo "  ${PASS} passed, ${FAIL} failed"
[ $FAIL -eq 0 ] || exit 1
