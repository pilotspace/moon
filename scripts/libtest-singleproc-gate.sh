#!/usr/bin/env bash
# libtest-singleproc-gate.sh -- run the lib unit tests in ONE process, the way
# `cargo nextest` never does, and grade the result against a COUNT baseline.
#
# ── Why this gate exists (moon#904) ──────────────────────────────────────
# Every configured test gate in this repo runs `cargo nextest run`: both VM
# suites and both --native suites in scripts/ci-local.sh, and the hosted Check
# leg in .github/workflows/ci.yml. nextest forks a PROCESS PER TEST. That is
# what makes it fast and what makes its retry profile trustworthy -- and it is
# also a structural blind spot, because a bug whose mechanism is
# PROCESS-GLOBAL STATE LEAKING FROM ONE TEST INTO ANOTHER never gets a second
# test to reach. Statics, OnceLock/OnceCell, env vars, global allocator state,
# installed hooks, process-wide registries: moon has several (memory_ctl, the
# Lua bridge, the tracing subscriber, jemalloc conf), and a regression in any
# of them lands green under nextest alone.
#
# Measured on moon-bench-arm (GCE t2a-standard-8, Linux aarch64), SAME commit
# f7c83769, SAME binary, SAME host:
#
#   cargo nextest run          PASS
#   cargo test --lib           FAIL -- scripting::bridge::tests::
#                                      gate_is_skipped_with_spill_sender_when_no_limit_is_configured
#
# Re-measured on the macOS host (aarch64, default features), SAME target dir
# and SAME test binary, at 65fa069e:
#
#   cargo nextest run --lib    5338 tests run: 5338 passed        rc=0
#   cargo test --lib           5337 passed; 1 failed              rc=101
#
# and at 7a87f69f, fifteen unit tests later: 5352 passed / 1 failed. That drift
# is exactly why the baseline below is a count that is REPORTED, not asserted.
#
# That is moon#856, and it is NOT macOS-specific as was believed -- what varies
# is the RUNNER, not the platform. nextest is kept for the other ~6000 tests;
# this adds the single-process run back beside it, for ~70-90s.
#
# ── What is ASSERTED, and what is only REPORTED ──────────────────────────
# ASSERTED: the suite reached its summary; it ran at least $MIN_LIBTEST_TESTS
# tests; there is at most $LIBTEST_KNOWN_FAILURES failure; and that failure is
# $LIBTEST_KNOWN_FAILURE by name. Count AND identity, because either alone is
# too loose: a count-only waiver waves a DIFFERENT single failure through
# (which is how "this suite always has that one red line" turns into a real
# regression shipping green), and a name-only one would not notice a second.
#
# REPORTED, gated on nothing: the passed count. It changes every time anyone
# adds a test, and it is NOT comparable across platforms -- Linux-only `cfg`
# code compiles Linux-only tests, so the same commit runs ~21 more of them on
# Linux than on macOS. A gate on that number would report a phantom +21 on a
# perfectly correct Linux run. `platform_baseline` below therefore prints the
# figure for the platform in hand, with the commit and date it was captured,
# so a human reads `baseline+N passed / 1 failed` and recognises "the known
# one" instead of a bare red.
#
# The identity is safe to assert despite the order-dependence: what varies is
# which SIBLING poisons the global, not which test carries the fragile
# assertion. Eight runs across two platforms and two runtimes name the same
# test. If it ever does move, the gate says so in those words rather than
# waving it through.
#
# ── DEVELOPER TRAP: re-running the suspect test alone is NOT a control ────
# These failures are order-dependent by construction. On pristine main the
# moon#856 test PASSES in isolation and fails only in the full --lib run. An
# engineer who isolates the red test sees green and concludes their own branch
# caused it -- the exact inversion of what the isolation check was for. To
# attribute a NEW failure, re-run this gate on the merge-base, not the test on
# its own.
#
# ── Maintaining the baseline ─────────────────────────────────────────────
# Two knobs, both env-overridable, both meant to be edited in this file when
# the facts change:
#
#   LIBTEST_KNOWN_FAILURES (default 1) -- how many failures moon#856 accounts
#     for. WHEN moon#856 IS FIXED, set this to 0 in this file; the gate then
#     fails on any failure at all. Until then a run with FEWER failures than
#     the waiver allows prints a NOTICE naming that exact edit (it does not
#     fail: an order-dependent bug can hide on a lucky scheduling, and a gate
#     that grounds healthy branches gets disabled, after which it guards
#     nothing).
#
#   LIBTEST_KNOWN_FAILURE -- the ONE test name the waiver covers. Retire it
#     together with the count above. If moon#856's failure ever lands on a
#     different test, this gate goes red and names both, which is the correct
#     outcome: that is either a new leak or the same one with a new victim.
#
#   platform_baseline() -- the printed reference counts, one branch per
#     `uname -s`/`uname -m`. Add a branch when the bar starts running on a new
#     platform; a platform with no branch prints "no baseline captured" rather
#     than quietly borrowing another platform's number.
#
#   MIN_LIBTEST_TESTS (default 5000) -- a floor on tests actually executed, so
#     a truncated or filtered run can never be waived. Checked BEFORE the
#     waiver, the same ordering scripts/consistency-gate.sh uses for moon#634.
#     The floor is deliberately well under the measured counts (5352 default
#     features, 4470 for runtime-tokio without graph/text-index) -- it is
#     there to catch a collapse, not to track drift. A leg with a smaller
#     feature set passes its own floor: MIN_LIBTEST_TESTS=4000 ./scripts/...
#
# Usage:  libtest-singleproc-gate.sh [extra args for `cargo test --lib`...]
#         libtest-singleproc-gate.sh --self-test
# Env:    MIN_LIBTEST_TESTS, LIBTEST_KNOWN_FAILURES, CARGO_TARGET_DIR

set -uo pipefail

MIN_LIBTEST_TESTS="${MIN_LIBTEST_TESTS:-5000}"
LIBTEST_KNOWN_FAILURES="${LIBTEST_KNOWN_FAILURES:-1}"
LIBTEST_KNOWN_FAILURE="${LIBTEST_KNOWN_FAILURE:-scripting::bridge::tests::gate_is_skipped_with_spill_sender_when_no_limit_is_configured}"

# The reference counts, keyed by PLATFORM, and printed for information only --
# nothing is gated on them (see the header). Keying matters because the counts
# are not comparable across platforms: Linux-only `cfg` code compiles
# Linux-only tests, so the same commit runs ~21 MORE of them there. A single
# hardcoded figure would make a correct Linux run read as "+21 unexplained
# tests" -- the exact phantom finding this stage exists to prevent.
platform_baseline() {
  case "$(uname -s) $(uname -m)" in
    "Darwin arm64")
      echo "baselines for macOS aarch64, captured 2026-09-10 at 7a87f69f:"
      echo "  5352 / 1   default features"
      echo "  4470 / 1   runtime-tokio,jemalloc (no graph, no text-index)" ;;
    "Linux aarch64")
      echo "baseline for Linux aarch64 (moon-bench-arm), captured at f7c83769:"
      echo "  5357 / 1   default features"
      echo "  the +21 over macOS is cfg(target_os = \"linux\") tests, not drift" ;;
    "Linux x86_64")
      echo "baseline for Linux x86_64 (hosted ubuntu Check leg), 2026-09-10:"
      echo "  4501 / 1   runtime-tokio,jemalloc (no graph, no text-index)"
      echo "  no default-features figure captured on this platform yet" ;;
    *)
      echo "no baseline captured for $(uname -s) $(uname -m). The count above is"
      echo "  informational and nothing is gated on it; add a branch to"
      echo "  platform_baseline() rather than borrowing another platform's." ;;
  esac
  echo "A branch adding N unit tests reads as baseline+N passed / 1 failed."
  echo "  The 1 is moon#856."
}

# Evaluate a captured transcript. Separated from running the suite so
# --self-test can exercise every verdict against synthetic transcripts --
# otherwise the only way to prove this gate CAN fail is to break the suite on
# purpose. (moon rule: a guard that has never been red is not evidence.)
evaluate_transcript() { # evaluate_transcript <transcript> <rc>
  local out="$1" rc="$2" line passed failed ignored ran

  # `--lib` builds exactly one test binary, so exactly one summary line. No
  # line at all means the run never reached its summary: a build failure, a
  # panic in a #[ctor], or an abort partway through. A gate that reads only
  # the exit code cannot tell that from a clean pass with rc=0 (moon#634).
  line="$(grep -E '^test result: ' <<< "$out" | tail -1)"
  if [ -z "$line" ]; then
    echo "GATE FAIL: the lib suite never printed a 'test result:' line." >&2
    echo "  The run did not reach its summary -- a build failure or an abort" >&2
    echo "  partway through, NOT a clean pass. See moon#634 for the class." >&2
    return 1
  fi

  passed="$(sed -E 's/.* ([0-9]+) passed.*/\1/'  <<< "$line")"
  failed="$(sed -E 's/.* ([0-9]+) failed.*/\1/'  <<< "$line")"
  ignored="$(sed -E 's/.* ([0-9]+) ignored.*/\1/' <<< "$line")"
  # A summary line that does not parse is a summary line we cannot grade.
  # sed leaves the subject untouched when the pattern does not match, so a
  # summary in an unexpected shape yields non-numeric junk rather than an
  # error. Refuse to grade it: a gate that cannot read its own result must
  # not return 0.
  case "$passed$failed" in
    ''|*[!0-9]*)
      echo "GATE FAIL: could not parse the summary line: $line" >&2
      return 1 ;;
  esac
  case "$ignored" in ''|*[!0-9]*) ignored="?" ;; esac
  ran=$((passed + failed))

  echo ""
  echo "  libtest (single process): ${passed} passed / ${failed} failed / ${ignored} ignored"
  platform_baseline | sed 's/^/    /'

  if [ "$ran" -lt "$MIN_LIBTEST_TESTS" ]; then
    echo "GATE FAIL: only ${ran} tests executed, expected >= ${MIN_LIBTEST_TESTS}." >&2
    echo "  The suite reached its summary but ran a fraction of itself (a stray" >&2
    echo "  filter, a feature set that dropped modules, a partial build)." >&2
    echo "  Checked BEFORE the waiver: a truncated run is never waivable." >&2
    return 1
  fi

  # libtest prints the failing test names in a plain list after the per-test
  # stdout dumps. Those lines are the only ones in the block that are bare
  # indented paths, so they are what identifies WHICH tests failed.
  local names unknown
  names="$(sed -n '/^failures:$/,/^test result: /p' <<< "$out" \
    | grep -E '^ +[a-z_][A-Za-z0-9_:]*$' | sed 's/^ *//' | sort -u)"
  unknown="$(grep -vxF "$LIBTEST_KNOWN_FAILURE" <<< "$names" | grep -v '^$')"

  if [ "$failed" -gt "$LIBTEST_KNOWN_FAILURES" ]; then
    echo "GATE FAIL: ${failed} failures, waiver covers at most ${LIBTEST_KNOWN_FAILURES}." >&2
    echo "  These are SINGLE-PROCESS failures: the mechanism is process-global" >&2
    echo "  state leaking between tests, so it is ORDER-DEPENDENT. Re-running" >&2
    echo "  the red test ALONE will very likely pass and prove nothing." >&2
    echo "  To attribute: run this same gate on the merge-base." >&2
    echo "  Failing tests:" >&2
    sed 's/^/   /' <<< "$names" >&2
    return 1
  fi

  # Count alone is not enough. A run with exactly one failure that is NOT the
  # known one is a DIFFERENT defect wearing the waiver's clothes, and waving it
  # through is how "this suite always has that one failure" turns into a real
  # regression shipping green. So the identity is asserted too. It is stable in
  # every measurement to date -- eight runs across two platforms and two
  # runtimes name the same test -- because the ORDER-DEPENDENT part is which
  # sibling poisons the global, not which test carries the fragile assertion.
  if [ -n "$unknown" ]; then
    echo "GATE FAIL: the single failure is not the one moon#856 accounts for." >&2
    echo "  waived : $LIBTEST_KNOWN_FAILURE" >&2
    echo "  got    :" >&2
    sed 's/^/           /' <<< "$unknown" >&2
    echo "  Either a new cross-test leak, or moon#856's poison landed on a" >&2
    echo "  different victim. Both deserve a look; neither is waivable. NOTE:" >&2
    echo "  re-running that test ALONE proves nothing -- these failures are" >&2
    echo "  order-dependent and pass in isolation. Run this gate on the" >&2
    echo "  merge-base to find out whether your branch caused it." >&2
    return 1
  fi

  # A non-zero cargo exit that the summary does not account for is a failure
  # this gate must not swallow: a link error in a later target, a signal after
  # the summary printed, a `cargo` error of its own. Grading purely on the
  # parsed counts would return 0 for all of them.
  if [ "$rc" -ne 0 ] && [ "$failed" -eq 0 ]; then
    echo "GATE FAIL: cargo exited rc=${rc} but the summary reports 0 failures." >&2
    echo "  Something failed OUTSIDE the test results -- a build or link error," >&2
    echo "  a signal, or cargo itself. Read the transcript above." >&2
    return 1
  fi

  if [ "$failed" -lt "$LIBTEST_KNOWN_FAILURES" ]; then
    echo ""
    echo "  NOTICE: ${failed} failure(s), but the waiver allows ${LIBTEST_KNOWN_FAILURES}."
    echo "    If moon#856 is fixed, set LIBTEST_KNOWN_FAILURES=0 at the top of"
    echo "    scripts/libtest-singleproc-gate.sh and close it. Not a failure"
    echo "    here: the bug is order-dependent and can hide on a lucky run, and"
    echo "    a gate that grounds healthy branches gets disabled."
    return 0
  fi

  if [ "$failed" -gt 0 ]; then
    echo ""
    echo "  WARNING: tolerating ${failed} known pre-existing failure(s) (moon#856:"
    echo "    a maxmemory predicate read from process-global state races sibling"
    echo "    tests in the same process). Any ADDITIONAL failure fails this gate."
  fi
  return 0
}

if [ "${1:-}" = "--self-test" ]; then
  fails=0
  check() { # check <name> <expected-rc> <transcript> <suite-rc> [env...]
    local name="$1" want="$2" transcript="$3" suiterc="$4"; shift 4
    local got
    got=$(env "$@" bash -c '
      set -uo pipefail
      MIN_LIBTEST_TESTS="${MIN_LIBTEST_TESTS:-5000}"
      LIBTEST_KNOWN_FAILURES="${LIBTEST_KNOWN_FAILURES:-1}"
      LIBTEST_KNOWN_FAILURE="${LIBTEST_KNOWN_FAILURE:-'"$LIBTEST_KNOWN_FAILURE"'}"
      '"$(declare -f platform_baseline)"'
      '"$(declare -f evaluate_transcript)"'
      evaluate_transcript "$1" "$2" >/dev/null 2>&1; echo $?' _ "$transcript" "$suiterc")
    if [ "$got" -eq "$want" ]; then echo "  ok    $name (rc=$got)"
    else echo "  FAIL  $name: wanted rc=$want, got rc=$got"; fails=$((fails + 1)); fi
  }

  # The real libtest failure block: per-test stdout dumps first, then a plain
  # indented list of names. Both `failures:` headings are reproduced so the
  # name extraction is exercised against the shape it actually meets.
  K="$LIBTEST_KNOWN_FAILURE"
  blk() { # blk <passed> <failed> <name...>
    local p="$1" f="$2"; shift 2
    printf 'failures:\n\n'
    local n; for n in "$@"; do printf -- '---- %s stdout ----\nthread panicked\n\n' "$n"; done
    printf 'failures:\n'
    for n in "$@"; do printf '    %s\n' "$n"; done
    printf '\ntest result: FAILED. %s passed; %s failed; 9 ignored; 0 measured; 0 filtered out\n' "$p" "$f"
  }

  clean="$(printf 'test result: ok. 5352 passed; 0 failed; 9 ignored; 0 measured; 0 filtered out\n')"
  known="$(blk 5352 1 "$K")"
  two="$(blk 5351 2 "$K" storage::tests::something_else)"
  other="$(blk 5352 1 storage::tests::something_else)"
  trunc="$(printf 'test scripting::bridge::tests::a ... ok\nerror: could not compile `moon`\n')"
  short="$(printf 'test result: ok. 200 passed; 0 failed; 0 ignored; 0 measured; 5138 filtered out\n')"
  shortfail="$(printf 'failures:\n    %s\n\ntest result: FAILED. 199 passed; 1 failed; 0 ignored; 0 measured; 5138 filtered out\n' "$K")"
  grew="$(blk 5400 1 "$K")"
  linux="$(blk 5357 1 "$K")"

  echo "libtest-singleproc-gate self-test:"
  check "clean run passes"                              0 "$clean"     0
  check "the one known moon#856 failure is waived"      0 "$known"     101
  # Count alone would waive this. Identity is what refuses it.
  check "a DIFFERENT single failure is NOT waived"      1 "$other"     101
  check "two failures are NOT waived"                   1 "$two"       101
  check "truncated run (no summary line) FAILS"         1 "$trunc"     101
  check "short run (200 < 5000 tests) FAILS"            1 "$short"     0
  check "short run that ALSO failed is not waived"      1 "$shortfail" 101
  check "a branch adding tests still reads as 1 failure" 0 "$grew"     101
  # The platform the count baseline does NOT come from: nothing is gated on
  # the count, so a Linux run with ~21 more tests must still pass.
  check "the Linux count (+21 cfg tests) is not a failure" 0 "$linux"  101
  check "with the waiver retired, one failure FAILS"    1 "$known"     101 LIBTEST_KNOWN_FAILURES=0
  check "with the waiver retired, a clean run passes"   0 "$clean"     0   LIBTEST_KNOWN_FAILURES=0
  check "a smaller feature set passes its own floor"    0 "$short"     0   MIN_LIBTEST_TESTS=100
  # rc=101 with a clean summary: something failed outside the test results.
  check "unexplained non-zero cargo exit is NOT swallowed" 1 "$clean"  101
  echo
  if [ "$fails" -ne 0 ]; then echo "SELF-TEST FAILED ($fails)"; exit 1; fi
  echo "SELF-TEST PASSED: the gate can fail, and only waives what it should."
  exit 0
fi

# NOTE: captured into a variable, never piped -- `cargo test | tee` reports
# tee's exit status, and a pipe also buffers, so a finished run looks unfinished.
out="$(cargo test --lib --no-fail-fast "$@" 2>&1)"
rc=$?
echo "$out"
evaluate_transcript "$out" "$rc"
