#!/usr/bin/env bash
# packaging/bump-homebrew.sh
#
# Renders packaging/homebrew/moon.rb.tmpl with real SHA256 digests and pushes
# the updated formula to the pilotspace/homebrew-moon tap repository.
#
# Required environment variables:
#   TAG                  — release tag, e.g. v0.2.0
#   HOMEBREW_TAP_TOKEN   — GitHub PAT with write access to pilotspace/homebrew-moon
#   GH_TOKEN             — GitHub token for `gh release download` (read-only)
#
# Usage (from repo root):
#   TAG=v0.2.0 HOMEBREW_TAP_TOKEN=... GH_TOKEN=... bash packaging/bump-homebrew.sh

set -euo pipefail

# ── Validation ──────────────────────────────────────────────────────────────
: "${TAG:?TAG environment variable is required (e.g. v0.2.0)}"
: "${HOMEBREW_TAP_TOKEN:?HOMEBREW_TAP_TOKEN environment variable is required}"
: "${GH_TOKEN:?GH_TOKEN environment variable is required}"

REPO="pilotspace/moon"
VERSION="${TAG#v}"   # strip leading 'v' → numeric, e.g. 0.2.0
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEMPLATE="${SCRIPT_DIR}/homebrew/moon.rb.tmpl"

if [ ! -f "$TEMPLATE" ]; then
  echo "Error: template not found at $TEMPLATE" >&2
  exit 1
fi

# ── Artifact names (must match release.yml matrix names exactly) ─────────────
AARCH64_MACOS="moon-${TAG}-aarch64-macos.tar.gz"
X86_64_MACOS="moon-${TAG}-x86_64-macos.tar.gz"
AARCH64_LINUX="moon-${TAG}-aarch64-linux-tokio.tar.gz"
X86_64_LINUX="moon-${TAG}-x86_64-linux-tokio.tar.gz"

TARBALLS=("$AARCH64_MACOS" "$X86_64_MACOS" "$AARCH64_LINUX" "$X86_64_LINUX")

# ── Download with retry/backoff ───────────────────────────────────────────────
download_with_retry() {
  local asset="$1"
  local max_attempts=3
  local attempt=1
  local delay=10

  while [ "$attempt" -le "$max_attempts" ]; do
    echo "Downloading $asset (attempt $attempt/$max_attempts)..."
    if gh release download "$TAG" \
        --repo "$REPO" \
        --pattern "$asset" \
        --dir /tmp/moon-homebrew-bump \
        --clobber 2>&1; then
      echo "Downloaded $asset"
      return 0
    fi

    if [ "$attempt" -lt "$max_attempts" ]; then
      echo "Download failed; retrying in ${delay}s..."
      sleep "$delay"
      delay=$(( delay * 2 ))
    fi
    attempt=$(( attempt + 1 ))
  done

  echo "Error: failed to download $asset after $max_attempts attempts" >&2
  return 1
}

# ── Setup workspace ───────────────────────────────────────────────────────────
WORK_DIR="/tmp/moon-homebrew-bump"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

# Download all 4 tarballs
for asset in "${TARBALLS[@]}"; do
  download_with_retry "$asset"
done

# ── Compute SHA256 digests ────────────────────────────────────────────────────
sha256_of() {
  local file="$WORK_DIR/$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  else
    # macOS
    shasum -a 256 "$file" | awk '{print $1}'
  fi
}

SHA_AARCH64_MACOS="$(sha256_of "$AARCH64_MACOS")"
SHA_X86_64_MACOS="$(sha256_of "$X86_64_MACOS")"
SHA_AARCH64_LINUX="$(sha256_of "$AARCH64_LINUX")"
SHA_X86_64_LINUX="$(sha256_of "$X86_64_LINUX")"

echo "SHA256 digests:"
echo "  aarch64-macos:   $SHA_AARCH64_MACOS"
echo "  x86_64-macos:    $SHA_X86_64_MACOS"
echo "  aarch64-linux:   $SHA_AARCH64_LINUX"
echo "  x86_64-linux:    $SHA_X86_64_LINUX"

# ── Render formula from template ──────────────────────────────────────────────
RENDERED="$WORK_DIR/moon.rb"

sed \
  -e "s/{{VERSION}}/${VERSION}/g" \
  -e "s/{{SHA_AARCH64_MACOS}}/${SHA_AARCH64_MACOS}/g" \
  -e "s/{{SHA_X86_64_MACOS}}/${SHA_X86_64_MACOS}/g" \
  -e "s/{{SHA_AARCH64_LINUX}}/${SHA_AARCH64_LINUX}/g" \
  -e "s/{{SHA_X86_64_LINUX}}/${SHA_X86_64_LINUX}/g" \
  "$TEMPLATE" > "$RENDERED"

echo "Rendered formula:"
cat "$RENDERED"

# ── Clone tap, update formula, push ──────────────────────────────────────────
TAP_DIR="$WORK_DIR/homebrew-moon"
TAP_REPO="https://x-access-token:${HOMEBREW_TAP_TOKEN}@github.com/pilotspace/homebrew-moon"

echo "Cloning tap repo..."
git clone --depth=1 "$TAP_REPO" "$TAP_DIR"

mkdir -p "$TAP_DIR/Formula"
cp "$RENDERED" "$TAP_DIR/Formula/moon.rb"

cd "$TAP_DIR"

git config user.email "github-actions[bot]@users.noreply.github.com"
git config user.name "github-actions[bot]"

git add Formula/moon.rb

# Only commit + push if there are actual changes
if git diff --cached --quiet; then
  echo "Formula unchanged; nothing to push."
else
  git commit -m "moon ${VERSION}"
  git push origin main
  echo "Homebrew formula updated for moon ${VERSION}"
fi

# ── Cleanup ───────────────────────────────────────────────────────────────────
rm -rf "$WORK_DIR"
echo "Done."
