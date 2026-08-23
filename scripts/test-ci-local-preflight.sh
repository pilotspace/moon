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

# ── moon#661: the VM's number alone is not the answer ──────────────────
# On 2026-08-22 the pre-flight printed a green light on precisely the
# failure it was written to catch: the VM honestly reported 18G free while
# the macOS volume backing its auto-expanding image was 100% full. Nothing
# could be written, so both legs exited rc=1 with no error text at all.

expect_host() { # expect_host <label> <want> <host-avail-gb> <vm-growth-gb>
  local label=$1 want=$2 avail=$3 growth=$4 got rc
  got=$(host_disk_verdict $((avail * G)) $((growth * G)))
  rc=$?
  if [ "$got" = "$want" ] && { [ "$want" != "FAIL" ] || [ $rc -ne 0 ]; } \
     && { [ "$want" = "FAIL" ] || [ $rc -eq 0 ]; }; then
    PASS=$((PASS + 1)); printf "  ok    %-46s %s\n" "$label" "$got"
  else
    FAIL=$((FAIL + 1)); printf "  FAIL  %-46s got=%s(rc=%s) want=%s\n" "$label" "$got" "$rc" "$want"
  fi
}

expect_growth() { # expect_growth <label> <want-gb> <tgt1-gb> <tgt2-gb>
  local label=$1 want=$2 t1=$3 t2=$4 got
  got=$(vm_growth_bytes $((t1 * G)) $((t2 * G)))
  if [ "$got" = "$((want * G))" ]; then
    PASS=$((PASS + 1)); printf "  ok    %-46s %sG\n" "$label" "$want"
  else
    FAIL=$((FAIL + 1)); printf "  FAIL  %-46s got=%sG want=%sG\n" "$label" "$((got / G))" "$want"
  fi
}

echo ""
echo "── vm_growth_bytes (what the image will claim from the host) ──"
# Measured 2026-08-23: one warm tokio leg grew the image 90.2 -> 90.5 GB.
# A cold leg is the expensive one -- it materializes a whole target dir.
expect_growth "two warm legs (cheap: nothing to build)"  4 34 34
expect_growth "one cold, one warm"                      38  0 34
expect_growth "two cold legs"                           72  0  0
expect_growth "a stub dir counts as cold"               38  2 34

echo ""
echo "── host_disk_verdict ──"
# The wall itself: 4.1G of 460G, and the VM saw 18G free the whole time.
expect_host "4G host free, warm run needs 4G (the 661 wall)" FAIL   4  4
expect_host "9G host free, nothing to build"                FAIL   9  0
# The regression this pair pins: a warm run on today's host must NOT be
# refused. An earlier draft charged 12G per warm leg and grounded a machine
# where ci-local had just passed.
expect_host "33G host free, warm run needs 4G (today)"      OK    33  4
expect_host "20G host free, warm run needs 4G"              WARN  20  4
# A cold run is the one that genuinely needs room for two target dirs.
expect_host "60G host free, cold run needs 72G"             FAIL  60 72
expect_host "85G host free, cold run needs 72G"             WARN  85 72
expect_host "100G host free, cold run needs 72G"            OK   100 72

echo ""
echo "── read_host_avail_bytes ──"
# `df -PB1` is a GNU spelling: macOS df rejects -B and prints usage. A probe
# written that way reads empty forever, and under the "cannot measure -> step
# aside" policy that is a guard which never runs.
#
# This block runs on the Linux CI lint runner as well as on a macOS host, and
# `$HOST_VOLUME` only exists on macOS -- so the portable assertion points the
# probe at a volume BOTH have. A first draft asserted the default path read a
# real number, which is true on the machine that runs ci-local and false on
# the runner that gates the PR: 18 passed, 1 failed, and the failure said
# nothing about df at all.
expect_reads() { # expect_reads <label> <volume> <want: number|empty>
  local label=$1 vol=$2 want=$3 got
  got=$(HOST_VOLUME=$vol read_host_avail_bytes)
  case "$want" in
    number)
      if [ -n "$got" ] && [ "$got" -gt 0 ] 2>/dev/null; then
        PASS=$((PASS + 1)); printf "  ok    %-46s %sG\n" "$label" "$((got / G))"
      else
        FAIL=$((FAIL + 1)); printf "  FAIL  %-46s got=%s\n" "$label" "${got:-<empty>}"
      fi ;;
    empty)
      if [ -z "$got" ]; then
        PASS=$((PASS + 1)); printf "  ok    %-46s <empty>\n" "$label"
      else
        FAIL=$((FAIL + 1)); printf "  FAIL  %-46s got=%s want=<empty>\n" "$label" "$got"
      fi ;;
  esac
}

# Portable: `/` is mounted on every host this ever runs on. This is the arm
# that DISCRIMINATES on macOS, where `df -PB1` prints usage and reads empty.
# On the Linux runner GNU df accepts both spellings, so there it is only a
# smoke test that the probe parses a df table at all -- worth stating, since
# a green Linux Lint leg is not evidence the macOS spelling is right.
expect_reads "reads a real number from a mounted volume" / number
# A volume that is not there reads empty, which is what makes preflight_disk
# print `host NOT CHECKED` instead of inventing a verdict.
expect_reads "an absent volume reads empty"              /no/such/volume empty

# The real macOS volume, only where it exists. Asserting this unconditionally
# is the bug described above.
if [ -d /System/Volumes/Data ]; then
  expect_reads "reads the macOS Data volume"             /System/Volumes/Data number
else
  printf "  skip  %-46s (not macOS)\n" "reads the macOS Data volume"
fi

echo ""
echo "  ${PASS} passed, ${FAIL} failed"
[ $FAIL -eq 0 ] || exit 1
