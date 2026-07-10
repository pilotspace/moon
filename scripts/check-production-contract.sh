#!/usr/bin/env bash
# check-production-contract.sh — Grep-based gate over docs/PRODUCTION-CONTRACT.md.
#
# The contract is a Markdown ledger of rows shaped like:
#   | <status> | <ID> | <item> | <evidence> | <blocking> |
# where <status> is "✅" (shipped) or "⬜" (not shipped), and <blocking> is
# "GA" (must be ticked before v1.0) or "—" (never blocks a tag).
#
# Modes:
#   - report (default): print the ledger summary and unticked GA-blocking
#     rows, exit 0 regardless of count. This is what every v0.x tag gets —
#     Moon is pre-GA, so an honest gap list is the point, not a failure.
#   - enforce: exit 1 if any GA-blocking row is unticked. Selected
#     automatically when --tag/$GITHUB_REF_NAME/$RELEASE_TAG matches a v1.x
#     tag (v1.0, v1.0.0, v1.0.0-rc1, v1.2.3, ...), or forced with --enforce.
#
# Usage:
#   ./scripts/check-production-contract.sh                    # report mode
#   ./scripts/check-production-contract.sh --tag v0.6.1        # report mode
#   ./scripts/check-production-contract.sh --tag v1.0.0        # enforce mode
#   ./scripts/check-production-contract.sh --enforce           # force enforce
#   ./scripts/check-production-contract.sh --file path/to.md   # override doc path
#
# CI: wired into .github/workflows/release.yml (setup job), mirroring the
# "Require RELEASES.md entry for this tag" release-ledger gate.

set -euo pipefail

FILE="docs/PRODUCTION-CONTRACT.md"
TAG="${GITHUB_REF_NAME:-${RELEASE_TAG:-}}"
FORCE_ENFORCE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --tag)
            TAG="$2"
            shift 2
            ;;
        --file)
            FILE="$2"
            shift 2
            ;;
        --enforce)
            FORCE_ENFORCE=1
            shift
            ;;
        -h|--help)
            sed -n '2,26p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

if [ ! -f "$FILE" ]; then
    echo "::error::$FILE not found" >&2
    exit 2
fi

# A GA-blocking row: "| <status> | ID | item | evidence | GA |"
# Anything else (blocking column "—") never gates a tag.
DONE_GA=$(grep -cE '^\| ✅ \| [A-Za-z0-9_.-]+ \|.*\| GA \|$' "$FILE" || true)
TODO_GA_LINES=$(grep -nE '^\| ⬜ \| [A-Za-z0-9_.-]+ \|.*\| GA \|$' "$FILE" || true)
TODO_GA=0
if [ -n "$TODO_GA_LINES" ]; then
    TODO_GA=$(printf '%s\n' "$TODO_GA_LINES" | wc -l | tr -d ' ')
fi
TOTAL_GA=$((DONE_GA + TODO_GA))

ENFORCE=0
if [ "$FORCE_ENFORCE" -eq 1 ]; then
    ENFORCE=1
elif printf '%s' "$TAG" | grep -qE '^v1\.'; then
    ENFORCE=1
fi

echo "=== Production Contract Ledger ($FILE) ==="
echo "GA-blocking rows shipped:   $DONE_GA / $TOTAL_GA"
echo "GA-blocking rows unticked:  $TODO_GA"
if [ -n "${TAG:-}" ]; then
    echo "Tag under evaluation:       $TAG"
fi
if [ "$ENFORCE" -eq 1 ]; then
    echo "Mode:                        ENFORCE (v1.0 gate — unticked GA rows fail the release)"
else
    echo "Mode:                        REPORT (pre-v1.0 — unticked GA rows are visible, not fatal)"
fi

if [ "$TODO_GA" -gt 0 ]; then
    echo ""
    echo "Unticked GA-blocking rows:"
    printf '%s\n' "$TODO_GA_LINES" | sed -E 's/^([0-9]+):\| ⬜ \| ([A-Za-z0-9_.-]+) \|.*/  - \2 (docs\/PRODUCTION-CONTRACT.md:\1)/'
fi

echo ""
if [ "$TODO_GA" -gt 0 ] && [ "$ENFORCE" -eq 1 ]; then
    echo "::error::$TODO_GA GA-blocking row(s) in $FILE are unticked at a v1.0 tag. Land the work or"
    echo "::error::demote the row's Blocking column to '—' with a documented reason before tagging v1.0."
    exit 1
fi

echo "PASSED (${TODO_GA} GA-blocking row(s) unticked; not gated at this tag)."
exit 0
