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
# That was moon#856, and it was NOT macOS-specific as was believed -- what
# varied is the RUNNER, not the platform. nextest is kept for the other ~6000
# tests; this adds the single-process run back beside it, for ~70-90s.
#
# moon#856 is now FIXED and its waiver is RETIRED (see LIBTEST_KNOWN_FAILURES
# below). It was never a race: `command::config::config_set` publishes five
# process-global memory-limit atomics on the CONFIG SET path and its unit tests
# never restored them, so the leak was PERMANENT and only the victim varied.
# `storage::eviction::PublishedLimits` scopes them now. This gate waives
# nothing; a single failure is a failure.
#
# ── What is ASSERTED, and what is only REPORTED ──────────────────────────
# ASSERTED: the suite reached its summary; it ran at least $MIN_LIBTEST_TESTS
# tests; there is at most $LIBTEST_KNOWN_FAILURES failure (now 0); and, if a
# waiver is ever re-armed, that failure is $LIBTEST_KNOWN_FAILURE by name.
# Count AND identity, because either alone is too loose: a count-only waiver
# waves a DIFFERENT single failure through (which is how "this suite always has
# that one red line" turns into a real regression shipping green), and a
# name-only one would not notice a second.
#
# REPORTED, gated on nothing: the passed count. It changes every time anyone
# adds a test, and it is NOT comparable across platforms -- Linux-only `cfg`
# code compiles Linux-only tests, so the same commit runs ~21 more of them on
# Linux than on macOS. A gate on that number would report a phantom +21 on a
# perfectly correct Linux run. `platform_baseline` below therefore prints the
# figure for the platform in hand, with the commit and date it was captured,
# so a human reads `baseline+N passed / 0 failed` and recognises a healthy run
# that simply added N tests, instead of an unexplained count.
#
# The identity was safe to assert despite the order-dependence: what varied is
# which SIBLING poisons the global, not which test carries the fragile
# assertion. Eight runs across two platforms and two runtimes named the same
# test.
#
# ── DEVELOPER TRAP: re-running the suspect test alone is NOT a control ────
# Cross-test-leak failures are order-dependent by construction. moon#856's
# victim PASSED in isolation and failed only in the full --lib run. An engineer
# who isolates the red test sees green and concludes their own branch caused it
# -- the exact inversion of what the isolation check was for. To attribute a
# NEW failure, re-run this gate on the merge-base, not the test on its own.
#
# ── Maintaining the baseline ─────────────────────────────────────────────
# Two knobs, both env-overridable, both meant to be edited in this file when
# the facts change:
#
#   LIBTEST_KNOWN_FAILURES (default 0 since moon#856 was fixed) -- how many
#     failures a waiver accounts for. At 0 the gate fails on any failure at
#     all, which is where it should stay. If a new cross-test leak ever has to
#     be tolerated, raise it AND set LIBTEST_KNOWN_FAILURE, and file the issue.
#     A run with FEWER failures than a waiver allows prints a NOTICE naming the
#     edit that retires it (it does not fail: an order-dependent bug can hide
#     on a lucky scheduling, and a gate that grounds healthy branches gets
#     disabled, after which it guards nothing).
#
#   LIBTEST_KNOWN_FAILURE (default empty) -- the ONE test name a waiver covers.
#     Set and retired together with the count above. If the waived failure ever
#     lands on a different test, this gate goes red and names both, which is
#     the correct outcome: that is either a new leak or the same one with a new
#     victim.
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
# WAIVER RETIRED -- moon#856 is FIXED. `config_set` published five
# process-global memory-limit atomics and its unit tests never put them back;
# `storage::eviction::PublishedLimits` now scopes every one of them to the call
# that makes it. There is no known failure any more, so the gate waives NOTHING
# and any failure at all fails it.
#
# The two knobs stay, empty, rather than being deleted: they are the mechanism
# for waiving a known failure, and the self-test below proves that mechanism
# still refuses the things it should. If a NEW cross-test leak is ever found
# and has to be tolerated for a while, set BOTH -- a count-only waiver waves a
# different single failure through, which is how "that one red line is normal"
# turns into a real regression shipping green.
LIBTEST_KNOWN_FAILURES="${LIBTEST_KNOWN_FAILURES:-0}"
LIBTEST_KNOWN_FAILURE="${LIBTEST_KNOWN_FAILURE:-}"

# The reference counts, keyed by PLATFORM, and printed for information only --
# nothing is gated on them (see the header). Keying matters because the counts
# are not comparable across platforms: Linux-only `cfg` code compiles
# Linux-only tests, so the same commit runs ~21 MORE of them there. A single
# hardcoded figure would make a correct Linux run read as "+21 unexplained
# tests" -- the exact phantom finding this stage exists to prevent.
platform_baseline() {
  case "$(uname -s) $(uname -m)" in
    "Darwin arm64")
      echo "baselines for macOS aarch64, default features re-captured with"
      echo "  moon#856 fixed (a8eb2efc + the fix, 2026-09-16):"
      echo "  5559 / 0   default features  (was 5352 / 1 at 7a87f69f)"
      echo "  4470 / 1   runtime-tokio,jemalloc -- PRE-fix; the 1 is gone now" ;;
    "Linux aarch64")
      echo "baseline for Linux aarch64 (moon-bench-arm), captured at f7c83769:"
      echo "  5357 / 1   default features -- PRE-moon#856-fix; the 1 is gone now"
      echo "  the +21 over macOS is cfg(target_os = \"linux\") tests, not drift" ;;
    "Linux x86_64")
      echo "baseline for Linux x86_64 (hosted ubuntu Check leg), 2026-09-10:"
      echo "  4501 / 1   runtime-tokio,jemalloc -- PRE-moon#856-fix, now 0 failed"
      echo "  no default-features figure captured on this platform yet" ;;
    *)
      echo "no baseline captured for $(uname -s) $(uname -m). The count above is"
      echo "  informational and nothing is gated on it; add a branch to"
      echo "  platform_baseline() rather than borrowing another platform's." ;;
  esac
  echo "A branch adding N unit tests reads as baseline+N passed / 0 failed."
  echo "  Since moon#856 this gate waives nothing: any failure fails it."
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
  # With the waiver retired `LIBTEST_KNOWN_FAILURE` is empty, and an empty
  # pattern is not a reliable "match nothing" across greps -- spell the two
  # cases out rather than depend on it.
  if [ -n "$LIBTEST_KNOWN_FAILURE" ]; then
    unknown="$(grep -vxF "$LIBTEST_KNOWN_FAILURE" <<< "$names" | grep -v '^$')"
  else
    unknown="$(grep -v '^$' <<< "$names")"
  fi

  if [ "$failed" -gt "$LIBTEST_KNOWN_FAILURES" ]; then
    if [ "$LIBTEST_KNOWN_FAILURES" -eq 0 ]; then
      echo "GATE FAIL: ${failed} failure(s); this gate waives nothing (moon#856 is fixed)." >&2
    else
      echo "GATE FAIL: ${failed} failures, waiver covers at most ${LIBTEST_KNOWN_FAILURES}." >&2
    fi
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
    echo "    If the waived bug is fixed, set LIBTEST_KNOWN_FAILURES=0 at the top"
    echo "    of scripts/libtest-singleproc-gate.sh, clear LIBTEST_KNOWN_FAILURE"
    echo "    and close it. Not a failure here: a cross-test leak is"
    echo "    order-dependent and can hide on a lucky run, and a gate that"
    echo "    grounds healthy branches gets disabled."
    return 0
  fi

  if [ "$failed" -gt 0 ]; then
    echo ""
    echo "  WARNING: tolerating ${failed} known pre-existing failure(s):"
    echo "    ${LIBTEST_KNOWN_FAILURE}"
    echo "    Any ADDITIONAL or DIFFERENT failure fails this gate."
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
      # Mirror the defaults at the top of this file, which are the RETIRED
      # ones since moon#856: no waiver, no waived name.
      LIBTEST_KNOWN_FAILURES="${LIBTEST_KNOWN_FAILURES:-0}"
      LIBTEST_KNOWN_FAILURE="${LIBTEST_KNOWN_FAILURE:-}"
      '"$(declare -f platform_baseline)"'
      '"$(declare -f evaluate_transcript)"'
      evaluate_transcript "$1" "$2" >/dev/null 2>&1; echo $?' _ "$transcript" "$suiterc")
    if [ "$got" -eq "$want" ]; then echo "  ok    $name (rc=$got)"
    else echo "  FAIL  $name: wanted rc=$want, got rc=$got"; fails=$((fails + 1)); fi
  }

  # The real libtest failure block: per-test stdout dumps first, then a plain
  # indented list of names. Both `failures:` headings are reproduced so the
  # name extraction is exercised against the shape it actually meets.
  # The test moon#856 used to redden. The waiver that named it is retired, so
  # this is now just a realistic test name for the synthetic transcripts -- and
  # for the cases that RE-ARM a waiver via the environment, to prove the
  # mechanism still refuses what it should.
  K="scripting::bridge::tests::gate_is_skipped_with_spill_sender_when_no_limit_is_configured"
  blk() { # blk <passed> <failed> <name...>
    local p="$1" f="$2"; shift 2
    printf 'failures:\n\n'
    local n; for n in "$@"; do printf -- '---- %s stdout ----\nthread panicked\n\n' "$n"; done
    printf 'failures:\n'
    for n in "$@"; do printf '    %s\n' "$n"; done
    printf '\ntest result: FAILED. %s passed; %s failed; 9 ignored; 0 measured; 0 filtered out\n' "$p" "$f"
  }

  clean="$(printf 'test result: ok. 5559 passed; 0 failed; 9 ignored; 0 measured; 0 filtered out\n')"
  known="$(blk 5558 1 "$K")"
  two="$(blk 5557 2 "$K" storage::tests::something_else)"
  other="$(blk 5558 1 storage::tests::something_else)"
  trunc="$(printf 'test scripting::bridge::tests::a ... ok\nerror: could not compile `moon`\n')"
  short="$(printf 'test result: ok. 200 passed; 0 failed; 0 ignored; 0 measured; 5138 filtered out\n')"
  shortfail="$(printf 'failures:\n    %s\n\ntest result: FAILED. 199 passed; 1 failed; 0 ignored; 0 measured; 5138 filtered out\n' "$K")"
  grew="$(printf 'test result: ok. 5600 passed; 0 failed; 9 ignored; 0 measured; 0 filtered out\n')"
  linux="$(printf 'test result: ok. 5580 passed; 0 failed; 9 ignored; 0 measured; 0 filtered out\n')"

  echo "libtest-singleproc-gate self-test:"
  check "clean run passes"                              0 "$clean"     0
  # THE retirement check (moon#856): with no waiver armed -- the shipped
  # default -- the failure this gate used to wave through now fails it. If this
  # line ever reads ok=0, the waiver has crept back in.
  check "the retired waiver no longer waives moon#856's victim" 1 "$known" 101
  check "any single failure FAILS with no waiver armed" 1 "$other"     101
  check "two failures FAIL with no waiver armed"        1 "$two"       101
  check "truncated run (no summary line) FAILS"         1 "$trunc"     101
  check "short run (200 < 5000 tests) FAILS"            1 "$short"     0
  check "short run that ALSO failed is not waived"      1 "$shortfail" 101
  check "a branch adding tests still passes"            0 "$grew"      0
  # The platform the count baseline does NOT come from: nothing is gated on
  # the count, so a Linux run with ~21 more tests must still pass.
  check "the Linux count (+21 cfg tests) is not a failure" 0 "$linux"  0
  # The WAIVER MECHANISM itself, re-armed through the environment. It is kept
  # (empty) rather than deleted so a future known failure can be tolerated
  # deliberately -- these three prove it still grants and still refuses.
  check "a re-armed waiver waives its NAMED failure"    0 "$known"     101 \
      LIBTEST_KNOWN_FAILURES=1 "LIBTEST_KNOWN_FAILURE=$K"
  # Count alone would waive this. Identity is what refuses it.
  check "a re-armed waiver refuses a DIFFERENT failure" 1 "$other"     101 \
      LIBTEST_KNOWN_FAILURES=1 "LIBTEST_KNOWN_FAILURE=$K"
  check "a re-armed waiver refuses a SECOND failure"    1 "$two"       101 \
      LIBTEST_KNOWN_FAILURES=1 "LIBTEST_KNOWN_FAILURE=$K"
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
