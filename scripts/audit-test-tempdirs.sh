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

# Two shapes are violations, and the second is the one this guard originally
# missed:
#
#   1. chained   — `std::env::temp_dir().join("fixed_name")`
#   2. split     — `let dir = std::env::temp_dir();`
#                  `let path = dir.join("fixed_name");`
#
# Shape 2 is not exotic: three of the ten sites moon#822 fixed were written
# that way (src/config/conf_file.rs). A grep that only sees shape 1 would have
# passed the very tree that motivated it, so it is checked by a small stateful
# pass that remembers which locals were bound from `temp_dir()`.
#
# `format!(...)` arguments are fine in both shapes: they are how pid, timestamp
# and counter get into the name.
scan() {
    local root="$1"
    # Shape 1. Comment lines are dropped afterwards: this script's own
    # rationale, and the doc comment on `unique_test_dir`, both quote the bad
    # pattern in prose. Dropping `//`, `//!` and `*` continuation lines keeps
    # the guard honest about CODE without banning the words that explain it.
    grep -rn --include='*.rs' -E 'temp_dir\(\)[[:space:]]*\.join\("' "$root" 2>/dev/null \
        | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|\*)'

    # Shape 2. Per file, track locals bound directly from `temp_dir()` with no
    # name of their own, then report `<local>.join("literal")`. The binding set
    # is reset at each `fn ` so a name reused in another function cannot carry
    # a stale mark across. Assignment to something else clears the mark.
    find "$root" -name '*.rs' -type f 2>/dev/null | while IFS= read -r f; do
        awk -v F="$f" '
            { line = $0 }
            line ~ /^[[:space:]]*(\/\/|\*)/ { next }
            line ~ /(^|[^a-zA-Z0-9_])fn[[:space:]]/ { delete marked }
            # `let x = ...temp_dir();` with nothing appended -> x is a bare tmpdir
            match(line, /let[[:space:]]+(mut[[:space:]]+)?[a-zA-Z_][a-zA-Z0-9_]*[[:space:]]*=[^;]*temp_dir\(\)[[:space:]]*;/) {
                s = substr(line, RSTART, RLENGTH)
                sub(/^let[[:space:]]+/, "", s); sub(/^mut[[:space:]]+/, "", s)
                sub(/[[:space:]]*=.*$/, "", s)
                marked[s] = 1
                next
            }
            # Rebinding the same name to anything else drops the mark.
            match(line, /let[[:space:]]+(mut[[:space:]]+)?[a-zA-Z_][a-zA-Z0-9_]*[[:space:]]*=/) {
                s = substr(line, RSTART, RLENGTH)
                sub(/^let[[:space:]]+/, "", s); sub(/^mut[[:space:]]+/, "", s)
                sub(/[[:space:]]*=.*$/, "", s)
                if (s in marked) delete marked[s]
            }
            {
                for (v in marked)
                    if (line ~ ("(^|[^a-zA-Z0-9_])" v "[[:space:]]*\\.join\\(\"")) {
                        printf "%s:%d:%s\n", F, FNR, line
                        break
                    }
            }
        ' "$f"
    done
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

    # The shape the first version of this guard could not see. Three of the ten
    # sites moon#822 fixed were written this way, so a guard that misses it
    # would have passed the tree it was written for.
    cat > "$tmp/src/planted.rs" <<'EOF'
fn f() {
    let dir = std::env::temp_dir();
    let path = dir.join("planted_split_binding.conf");
}
EOF
    if scan "$tmp/src" | grep -q planted_split_binding; then
        echo "SELF-TEST PASS: scanner reports a split \`let dir = temp_dir()\` binding"
    else
        echo "SELF-TEST FAIL: scanner did NOT report a split temp_dir binding" >&2
        exit 1
    fi

    # ...and the same shape stays clean when the name carries a runtime part,
    # so the new rule cannot be satisfied by banning `.join(` outright.
    cat > "$tmp/src/planted.rs" <<'EOF'
fn f() {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("ok-split-{}", std::process::id()));
}
EOF
    if scan "$tmp/src" | grep -q 'ok-split'; then
        echo "SELF-TEST FAIL: scanner flagged a format!()-built split path" >&2
        exit 1
    fi
    echo "SELF-TEST PASS: scanner does not flag a format!()-built split path"

    # A name rebound to something else must lose its mark, or every later
    # `.join("literal")` in the function becomes a false positive.
    cat > "$tmp/src/planted.rs" <<'EOF'
fn f() {
    let dir = std::env::temp_dir();
    let dir = other_root();
    let path = dir.join("not_a_tempdir_child");
}
EOF
    if scan "$tmp/src" | grep -q not_a_tempdir_child; then
        echo "SELF-TEST FAIL: scanner flagged a rebound, non-tempdir local" >&2
        exit 1
    fi
    echo "SELF-TEST PASS: scanner drops the mark when the local is rebound"
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
