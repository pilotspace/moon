#!/usr/bin/env bash
# audit-test-tempdirs.sh — no fixed-name scratch paths under src/ (moon#822).
#
# A test that names its scratch path with a string literal is sharing a
# process-global resource with every other `cargo test` process on the host.
# `scripts/ci-local.sh` runs the monoio and tokio VM suites CONCURRENTLY by
# default, so this is the normal case, not an exotic one. Measured at
# ae6cd003, two concurrent processes looping one test:
#
#   persistence::kv_page::tests::test_datafile_roundtrip  0/40 solo, 13/80 concurrent
#   tls::tests::test_reload_tls_config_swaps_config       0/20 solo, 24/50 concurrent
#
# The TLS one does not say "missing file". It says
# `TLS config: keys may not be consistent: KeyMismatch` — one run reading
# ANOTHER run's cert. A harness defect that reads as a TLS bug.
#
# The fix is `crate::util::test_temp::unique_test_dir(prefix)` (lib tests) or
# `common::unique_test_dir(prefix)` (integration tests). Both append pid, a
# nanosecond timestamp, and a process-local counter — the counter is the part
# that cannot collide, because macOS `SystemTime::now()` has only microsecond
# resolution.
#
# Run: ./scripts/audit-test-tempdirs.sh
# Self-test (proves the guard can fail): ./scripts/audit-test-tempdirs.sh --self-test

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

# A violation is `temp_dir()` followed by `.join("` with a bare literal, or
# `.join(dir_literal)` where the literal carries no runtime component.
# `format!(...)` arguments are fine: they are how pid/counter get in.
scan() {
    local root="$1"
    # -n for file:line. The pattern deliberately allows whitespace/newline-free
    # chaining only; a multi-line `.join(` split across lines is rare enough
    # that a false negative there is acceptable — a false POSITIVE is not.
    #
    # Comment lines are dropped afterwards: this script's own rationale, and
    # the doc comment on `unique_test_dir`, both quote the bad pattern in
    # prose. Dropping `//`, `//!` and `*` continuation lines keeps the guard
    # honest about CODE without banning the words that explain it.
    grep -rn --include='*.rs' -E 'temp_dir\(\)[[:space:]]*\.join\("' "$root" 2>/dev/null \
        | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|\*)'
}

VIOLATIONS="$(scan src)"

if [ "${1:-}" = "--self-test" ]; then
    # Plant a violation in a scratch tree and assert the scanner reports it.
    # A guard that has never been observed failing is not a guard.
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    mkdir -p "$tmp/src"
    printf 'fn f() { let d = std::env::temp_dir().join("planted_fixed_name"); }\n' \
        > "$tmp/src/planted.rs"
    if scan "$tmp/src" | grep -q planted_fixed_name; then
        echo "SELF-TEST PASS: scanner reports a planted fixed-name temp path"
    else
        echo "SELF-TEST FAIL: scanner did NOT report a planted violation" >&2
        exit 1
    fi
    printf 'fn f() { let d = std::env::temp_dir().join(format!("ok-{}", 1)); }\n' \
        > "$tmp/src/planted.rs"
    if scan "$tmp/src" | grep -q 'ok-'; then
        echo "SELF-TEST FAIL: scanner flagged a format!()-built path" >&2
        exit 1
    fi
    echo "SELF-TEST PASS: scanner does not flag a format!()-built path"
    exit 0
fi

if [ -n "$VIOLATIONS" ]; then
    echo "FAIL: fixed-name temp paths under src/ (moon#822)."
    echo
    echo "$VIOLATIONS"
    echo
    echo "Every concurrent 'cargo test' process on the host picks the same path"
    echo "and one run's teardown deletes it under another's feet."
    echo "Use crate::util::test_temp::unique_test_dir(prefix) instead."
    exit 1
fi

echo "PASS: no fixed-name temp paths under src/"
exit 0
