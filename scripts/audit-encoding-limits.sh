#!/usr/bin/env bash
# audit-encoding-limits.sh — the compact-encoding thresholds have ONE authority
# (moon#896), and nothing reaches around it.
#
# Before `storage::encoding_limits::EncodingLimits`, three sites decided
# whether a container stays in its compact form, each with its own copy of
# the arithmetic, and two of them disagreed about the UNIT: the entry gate
# compared `args.len() - 1` (listpack ENTRIES, two per hash field) against a
# threshold the upgrade check applied to `lp.len() / 2` (fields). A bulk
# `HSET` of 65 fields promoted at half the intended cardinality.
#
# The authority makes that impossible only while every consultation goes
# through it. This guard keeps it that way:
#
#   1. The retired names must not come back anywhere in `src/` outside the
#      authority module: `LISTPACK_MAX_ENTRIES`, `LISTPACK_MAX_ELEMENT_SIZE`,
#      `INTSET_MAX_ENTRIES`, `listpack_batch_fits`. The constants are private
#      to the module, so the compiler already refuses the path form; this
#      catches a re-declaration.
#   2. In the consultation directories, an upgrade decision must be the
#      authority's: a `should_upgrade =` line that does not call
#      `listpack_fits(` is a hand-rolled threshold.
#   3. In the consultation directories, no code line re-derives the unit —
#      `.len() / 2` compared with anything is exactly the moon#896 shape.
#
# Comment lines (`//`, `//!`, `///`, ` *`) are dropped before matching so the
# rationale can name the patterns it bans.
#
# Run:       ./scripts/audit-encoding-limits.sh
# Self-test: ./scripts/audit-encoding-limits.sh --self-test   (proves it can fail)

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

AUTHORITY="src/storage/encoding_limits.rs"
# Where the policy is consulted: every write command, the codec's decode-side
# re-derivation, and the persistence loaders that call it.
CONSULT_DIRS=(src/command src/storage/value_codec.rs src/storage/db src/persistence)

# Print code lines (file:line:text) of `*.rs` under the given roots, with
# comment lines removed.
code_lines() {
    grep -rn --include='*.rs' -E '.' "$@" 2>/dev/null \
        | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|\*)'
}

scan() {
    local root="$1"
    local -a consult=()
    local d
    for d in "${CONSULT_DIRS[@]}"; do
        consult+=("$root/$d")
    done

    # Rule 1: retired names, anywhere under src/ except the authority.
    code_lines "$root/src" \
        | grep -E '\b(LISTPACK_MAX_ENTRIES|LISTPACK_MAX_ELEMENT_SIZE|INTSET_MAX_ENTRIES|listpack_batch_fits)\b' \
        | grep -v "/${AUTHORITY}:" \
        | sed 's/$/   <- retired threshold name; use EncodingLimits (rule 1)/'

    # Rule 2: an upgrade decision that is not the authority's.
    code_lines "${consult[@]}" \
        | grep -E 'should_upgrade[[:space:]]*=' \
        | grep -v 'listpack_fits(' \
        | sed 's/$/   <- upgrade check must be EncodingLimits::listpack_fits (rule 2)/'

    # Rule 3: a re-derived unit — `.len() / 2` compared with anything.
    code_lines "${consult[@]}" \
        | grep -E '\.len\(\)[[:space:]]*/[[:space:]]*2[[:space:]]*(>|<|>=|<=|==|!=)' \
        | sed 's/$/   <- unit re-derived by hand; use Shape::items_in (rule 3)/'
}

self_test() {
    local tmp
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/audit-encoding-limits.XXXXXX")"
    trap 'rm -rf "$tmp"' RETURN
    mkdir -p "$tmp/src/command/hash" "$tmp/src/storage" "$tmp/src/persistence" "$tmp/src/storage/db"

    # One violation of each rule, and a clean file that must NOT be flagged.
    cat > "$tmp/src/command/hash/bad1.rs" <<'EOF'
pub const LISTPACK_MAX_ENTRIES: usize = 128;
EOF
    cat > "$tmp/src/command/hash/bad2.rs" <<'EOF'
let should_upgrade = lp.len() > 128;
EOF
    cat > "$tmp/src/command/hash/bad3.rs" <<'EOF'
if lp.len() / 2 > limit { promote(); }
EOF
    cat > "$tmp/src/storage/bad4.rs" <<'EOF'
fn f() -> bool { listpack_batch_fits(3) }
EOF
    cat > "$tmp/src/command/hash/clean.rs" <<'EOF'
// LISTPACK_MAX_ENTRIES is only mentioned in this comment, which is fine.
let limits = db.encoding_limits();
let should_upgrade = !limits.listpack_fits(Shape::Hash, lp);
if limits.fits(Shape::Hash, Shape::Hash.items_in(args.len() - 1), max_elem) { }
EOF
    # The authority itself may name its constants.
    mkdir -p "$tmp/$(dirname "$AUTHORITY")"
    cat > "$tmp/$AUTHORITY" <<'EOF'
const HASH_MAX_LISTPACK_ENTRIES: usize = 128; // LISTPACK_MAX_ENTRIES successor
pub fn listpack_batch_fits_is_gone() {}
EOF

    local out rc=0
    out="$(scan "$tmp")"
    local want
    for want in bad1.rs bad2.rs bad3.rs bad4.rs; do
        if ! grep -q "$want" <<< "$out"; then
            echo "SELF-TEST FAIL: $want was not flagged"; rc=1
        fi
    done
    if grep -q 'clean.rs' <<< "$out"; then
        echo "SELF-TEST FAIL: clean.rs was flagged:"; grep 'clean.rs' <<< "$out"; rc=1
    fi
    if grep -q "encoding_limits.rs" <<< "$out"; then
        echo "SELF-TEST FAIL: the authority module was flagged:"; grep 'encoding_limits.rs' <<< "$out"; rc=1
    fi
    if [[ $rc -eq 0 ]]; then
        echo "audit-encoding-limits self-test: OK (4 violations flagged, 2 clean files passed)"
    fi
    return $rc
}

if [[ "${1:-}" == "--self-test" ]]; then
    self_test
    exit $?
fi

if [[ ! -f "$AUTHORITY" ]]; then
    echo "FAIL: $AUTHORITY is missing — the encoding authority was removed?"
    exit 1
fi

violations="$(scan "$REPO_ROOT")"
if [[ -n "$violations" ]]; then
    echo "FAIL: encoding thresholds consulted outside the authority (moon#896):"
    echo "$violations"
    echo
    echo "Every threshold decision goes through storage::encoding_limits::EncodingLimits:"
    echo "  entry gate    -> limits.fits(Shape::X, Shape::X.items_in(argv_len), max_elem)"
    echo "  upgrade check -> !limits.listpack_fits(Shape::X, lp)"
    echo "  decode        -> compact_after_decode_with(v, limits)"
    exit 1
fi
echo "audit-encoding-limits: OK"
