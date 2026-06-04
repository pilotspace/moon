#!/bin/sh
# install.sh — Moon installer for Linux and macOS
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/pilotspace/moon/main/install.sh | sh
#   VERSION=v0.2.0 INSTALL_DIR=~/.local/bin sh install.sh
#
# Environment overrides:
#   VERSION      — e.g. "v0.2.0" (default: latest release)
#   INSTALL_DIR  — installation directory (default: ~/.local/bin or
#                  /usr/local/bin when running as root)
set -euf

# ── Helpers ──────────────────────────────────────────────────────────────────

say()  { printf '%s\n' "$*"; }
warn() { printf 'warn: %s\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        die "required command not found: $1"
    fi
}

# ── Detect OS and arch ───────────────────────────────────────────────────────

detect_os() {
    case "$(uname -s)" in
        Linux)  printf 'linux'  ;;
        Darwin) printf 'macos'  ;;
        *)      die "unsupported OS: $(uname -s)" ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64)         printf 'x86_64'   ;;
        aarch64|arm64)  printf 'aarch64'  ;;
        *)              die "unsupported architecture: $(uname -m)" ;;
    esac
}

# Artifact runtime suffix per the release matrix:
#   linux/x86_64  → monoio (io_uring, max throughput)
#   linux/aarch64 → tokio
#   macos/*       → (no suffix; tokio)
runtime_suffix() {
    _os="$1"
    _arch="$2"
    if [ "$_os" = "linux" ] && [ "$_arch" = "x86_64" ]; then
        printf -- '-monoio'
    elif [ "$_os" = "linux" ] && [ "$_arch" = "aarch64" ]; then
        printf -- '-tokio'
    else
        printf ''
    fi
}

# ── Resolve latest version via redirect ─────────────────────────────────────
# Uses the GitHub releases/latest redirect — no API token, no rate limits.

resolve_latest_version() {
    need_cmd curl
    # curl -sI follows redirects and prints headers only.
    # HTTP/2 headers are lowercase; strip trailing CR with tr.
    _location=$(curl -sI https://github.com/pilotspace/moon/releases/latest \
        | tr -d '\r' \
        | grep -i '^location:' \
        | tail -1 \
        | sed 's/.*\/tag\///')
    if [ -z "$_location" ]; then
        die "could not determine latest version (no Location header in redirect)"
    fi
    printf '%s' "$_location"
}

# ── Checksum verification ────────────────────────────────────────────────────

verify_checksum() {
    _artifact="$1"
    _sums_file="$2"

    # Filter to only the line for our artifact to avoid "file not found" errors
    # for other platform artifacts listed in SHA256SUMS.txt.
    _line=$(grep " ${_artifact}\$" "$_sums_file" 2>/dev/null || true)
    if [ -z "$_line" ]; then
        die "artifact '${_artifact}' not found in ${_sums_file}"
    fi

    if command -v sha256sum >/dev/null 2>&1; then
        printf '%s\n' "$_line" | sha256sum -c - || die "checksum verification failed"
    elif command -v shasum >/dev/null 2>&1; then
        printf '%s\n' "$_line" | shasum -a 256 -c - || die "checksum verification failed"
    else
        warn "neither sha256sum nor shasum found — cannot verify download integrity"
        warn "Aborting install. Set SKIP_CHECKSUM=1 to override (not recommended)."
        if [ "${SKIP_CHECKSUM:-0}" != "1" ]; then
            die "aborting: install unverified artifacts is disabled by default"
        fi
    fi
}

# ── Main ─────────────────────────────────────────────────────────────────────

main() {
    need_cmd curl
    need_cmd tar

    OS=$(detect_os)
    ARCH=$(detect_arch)
    RUNTIME=$(runtime_suffix "$OS" "$ARCH")

    # Resolve version
    if [ -z "${VERSION:-}" ]; then
        say "Detecting latest Moon release…"
        VERSION=$(resolve_latest_version)
    fi
    say "Installing Moon ${VERSION} (${ARCH}-${OS}${RUNTIME})"

    # Determine install directory
    if [ -z "${INSTALL_DIR:-}" ]; then
        if [ "$(id -u)" = "0" ]; then
            INSTALL_DIR=/usr/local/bin
        else
            INSTALL_DIR="${HOME}/.local/bin"
        fi
    fi

    # Artifact and checksum filenames
    ARTIFACT="moon-${VERSION}-${ARCH}-${OS}${RUNTIME}.tar.gz"
    BASE_URL="https://github.com/pilotspace/moon/releases/download/${VERSION}"

    # Create temp dir with guaranteed cleanup
    TMPDIR=$(mktemp -d)
    trap 'rm -rf "$TMPDIR"' EXIT INT TERM

    # Download tarball
    say "Downloading ${ARTIFACT}…"
    curl -fSL "${BASE_URL}/${ARTIFACT}" -o "${TMPDIR}/${ARTIFACT}" \
        || die "download failed: ${BASE_URL}/${ARTIFACT}"

    # Download checksum file
    say "Downloading SHA256SUMS.txt…"
    curl -fSL "${BASE_URL}/SHA256SUMS.txt" -o "${TMPDIR}/SHA256SUMS.txt" \
        || die "download failed: ${BASE_URL}/SHA256SUMS.txt"

    # Verify checksum (runs in TMPDIR so relative path in sums file works)
    say "Verifying checksum…"
    (cd "$TMPDIR" && verify_checksum "$ARTIFACT" SHA256SUMS.txt)
    say "Checksum OK."

    # Extract
    say "Extracting…"
    tar -xzf "${TMPDIR}/${ARTIFACT}" -C "$TMPDIR"

    # Install binary
    mkdir -p "$INSTALL_DIR"
    install -m 755 "${TMPDIR}/moon" "${INSTALL_DIR}/moon" \
        || die "install failed — try running with sudo or set INSTALL_DIR"
    say "Installed moon to ${INSTALL_DIR}/moon"

    # PATH guidance
    case ":${PATH}:" in
        *":${INSTALL_DIR}:"*) ;;
        *)
            say ""
            say "  ${INSTALL_DIR} is not in your PATH."
            say "  Add it by running one of:"
            say "    export PATH=\"${INSTALL_DIR}:\$PATH\"          # current shell"
            say "    echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.bashrc   # bash"
            say "    echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.zshrc    # zsh"
            ;;
    esac

    say ""
    say "Quick start:"
    say "  moon --port 6379 --appendonly yes"
    say "  redis-cli ping"
    say ""
    say "Documentation: https://github.com/pilotspace/moon"
}

main "$@"
