#!/bin/bash
set -e

# Push to both remotes:
#   moon      → code only (excludes .planning/)
#   moon-docs → .planning/ only (via subtree)
#
# Usage:
#   ./push.sh          Push to both remotes
#   ./push.sh moon     Push code only
#   ./push.sh docs     Push .planning/ only

BRANCH=$(git branch --show-current)
TARGET="${1:-all}"

push_docs() {
    echo "=== Pushing .planning/ to moon-docs ==="
    git branch -D _planning-split 2>/dev/null || true
    git subtree split --prefix=.planning -b _planning-split
    git push --force moon-docs _planning-split:main
    git branch -D _planning-split
}

push_moon() {
    echo "=== Pushing code to moon (excluding .planning/) ==="
    local tmpdir
    tmpdir=$(mktemp -d)
    trap "git worktree remove --force '$tmpdir' 2>/dev/null; rm -rf '$tmpdir'" EXIT
    git worktree add --detach "$tmpdir" "$BRANCH" 2>/dev/null
    (
        cd "$tmpdir"
        git rm -r --cached .planning/ > /dev/null 2>&1
        git commit -m "temp: exclude .planning" > /dev/null 2>&1
        git push moon HEAD:main --force
    )
    git worktree remove --force "$tmpdir" 2>/dev/null
    trap - EXIT
}

case "$TARGET" in
    docs)  push_docs ;;
    moon)  push_moon ;;
    all)   push_docs; echo ""; push_moon ;;
    *)     echo "Usage: ./push.sh [moon|docs|all]"; exit 1 ;;
esac

echo ""
echo "Done."
