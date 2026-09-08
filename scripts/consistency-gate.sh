#!/usr/bin/env bash
# consistency-gate.sh -- run test-consistency.sh as a merge gate.
#
# ONE implementation, called by both the host leg and the VM leg of
# ci-local.sh. It used to be two hand-transcriptions of the same rc/count/
# waiver logic, so fixing the waiver or adding a check meant editing both and
# nothing failed if they drifted.
#
# Three things a bare exit code cannot tell you, and this does:
#
#   1. Did the suite REACH ITS SUMMARY? `test-consistency.sh` runs under
#      `set -euo pipefail`. An abort partway through can leave a zero exit and
#      zero `FAIL:` lines -- a gate that checks only those two reports success
#      for a run that executed a fraction of itself. moon#634 is exactly that:
#      a `set -u` abort had this script silently running ~half its rows for
#      months.
#   2. Did it run ENOUGH ROWS? The summary can be reached with far fewer
#      assertions than the suite contains if an early section is skipped.
#   3. Is the only failure the ONE known pre-existing one (moon#536)?
#
# Checks 1 and 2 run BEFORE the waiver, so a truncated run can never be waived.
#
# Usage:  consistency-gate.sh [args passed to test-consistency.sh...]
#         consistency-gate.sh --self-test
# Env:    MIN_CONSISTENCY_ROWS (default 458)

set -uo pipefail

MIN_ROWS="${MIN_CONSISTENCY_ROWS:-458}"
KNOWN_WAIVER='  FAIL: ROLE on a master'

# Evaluate a captured suite transcript. Separated from running the suite so
# --self-test can exercise it against synthetic transcripts -- otherwise the
# only way to test the gate is to make a real 458-row run fail on purpose.
evaluate_transcript() { # evaluate_transcript <transcript> <rc>
  local out="$1" rc="$2" total fails

  if ! grep -q '^  TOTAL:' <<< "$out"; then
    echo "GATE FAIL: the suite never reached its summary block (TRUNCATED RUN)." >&2
    echo "  A partial run exits 0 with no FAIL: lines and looks identical to a" >&2
    echo "  clean one. See moon#634." >&2
    return 1
  fi

  total="$(sed -n 's/^  TOTAL:[[:space:]]*\([0-9][0-9]*\).*/\1/p' <<< "$out" | tail -1)"
  if [ -z "$total" ] || [ "$total" -lt "$MIN_ROWS" ]; then
    echo "GATE FAIL: only ${total:-0} assertions ran, expected >= $MIN_ROWS." >&2
    echo "  The suite reached its summary but skipped rows." >&2
    return 1
  fi

  if [ "$rc" -ne 0 ]; then
    fails="$(grep -c '^  FAIL:' <<< "$out")"
    if [ "$fails" -eq 1 ] && grep -qxF "$KNOWN_WAIVER" <<< "$out"; then
      echo ""
      echo "  WARNING: tolerating the one known pre-existing failure (moon#536:"
      echo "    ROLE's replication offset on a masterless server -- unrelated to"
      echo "    FT.*). Any OTHER or ADDITIONAL failure still fails this gate."
      return 0
    fi
    return "$rc"
  fi
  return 0
}

if [ "${1:-}" = "--self-test" ]; then
  fails=0
  check() { # check <name> <expected-rc> <transcript> <suite-rc>
    local name="$1" want="$2" transcript="$3" suiterc="$4" got
    evaluate_transcript "$transcript" "$suiterc" >/dev/null 2>&1; got=$?
    if [ "$got" -eq "$want" ]; then echo "  ok    $name (rc=$got)"
    else echo "  FAIL  $name: wanted rc=$want, got rc=$got"; fails=$((fails + 1)); fi
  }
  full="$(printf '  PASSED: 458\n  FAILED: 0\n  TOTAL:  458\n')"
  waived="$(printf '%s\n  PASSED: 457\n  FAILED: 1\n  TOTAL:  458\n' "$KNOWN_WAIVER")"
  other="$(printf '  FAIL: AGG-03 cross-shard divergence\n  PASSED: 457\n  FAILED: 1\n  TOTAL:  458\n')"
  two="$(printf '%s\n  FAIL: AGG-03 cross-shard divergence\n  TOTAL:  458\n' "$KNOWN_WAIVER")"
  trunc="$(printf '  PASSED: 12\n')"
  short="$(printf '  PASSED: 200\n  FAILED: 0\n  TOTAL:  200\n')"

  echo "consistency-gate self-test:"
  check "clean run passes"                        0 "$full"   0
  check "known moon#536 failure is waived"        0 "$waived" 1
  check "a DIFFERENT single failure is not waived" 1 "$other"  1
  check "known failure PLUS another is not waived" 1 "$two"    1
  check "truncated run (no summary) FAILS"        1 "$trunc"  0
  check "short run (200 < 458 rows) FAILS"        1 "$short"  0
  echo
  if [ "$fails" -ne 0 ]; then echo "SELF-TEST FAILED ($fails)"; exit 1; fi
  echo "SELF-TEST PASSED: the gate can fail, and only waives what it should."
  exit 0
fi

out="$(./scripts/test-consistency.sh "$@" 2>&1)"
rc=$?
echo "$out"
evaluate_transcript "$out" "$rc"
