#!/usr/bin/env bash
# scripts/test-integration.sh — INT-03 live integration harness.
#
# Builds moon with --features console, seeds deterministic KV/vector/graph
# fixtures, runs Playwright against the running server, then tears down.
# Runs inside OrbStack moon-dev per CLAUDE.md (Linux-only).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADMIN_PORT="${ADMIN_PORT:-9100}"
RESP_PORT="${RESP_PORT:-6399}"
MOON_PID=""
MOON_LOG="${REPO_ROOT}/target/integration-moon.log"
MOON_BIN="${REPO_ROOT}/target/release/moon"

teardown() {
  set +e
  if [[ -n "${MOON_PID}" ]] && kill -0 "${MOON_PID}" 2>/dev/null; then
    echo "[integration] stopping moon pid=${MOON_PID}"
    kill -TERM "${MOON_PID}" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${MOON_PID}" 2>/dev/null || break
      sleep 0.5
    done
    kill -KILL "${MOON_PID}" 2>/dev/null || true
  fi
  rm -rf "${REPO_ROOT}/target/integration-data" || true
}
trap 'teardown' EXIT INT TERM

echo "[integration] building console frontend"
cd "${REPO_ROOT}/console"
pnpm install --frozen-lockfile
pnpm run build
cd "${REPO_ROOT}"

echo "[integration] building moon (release, --features console)"
cargo build --release --features console

echo "[integration] preparing data dir"
rm -rf "${REPO_ROOT}/target/integration-data"
mkdir -p "${REPO_ROOT}/target/integration-data"

echo "[integration] starting moon on :${RESP_PORT} / admin :${ADMIN_PORT}"
# Invocation shape (defaults): moon --port 6399 --admin-port 9100 --shards 4
"${MOON_BIN}" \
  --port "${RESP_PORT}" \
  --admin-port "${ADMIN_PORT}" \
  --shards 4 \
  --dir "${REPO_ROOT}/target/integration-data" \
  > "${MOON_LOG}" 2>&1 &
MOON_PID=$!

echo "[integration] waiting for /readyz"
for i in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${ADMIN_PORT}/readyz" >/dev/null 2>&1; then
    echo "[integration] moon ready after ${i}s"
    break
  fi
  if ! kill -0 "${MOON_PID}" 2>/dev/null; then
    echo "[integration] moon exited during startup; log:"
    tail -n 80 "${MOON_LOG}" || true
    exit 1
  fi
  sleep 1
done

echo "[integration] seeding fixtures"
python3 "${REPO_ROOT}/scripts/seed-console-fixtures.py" \
  --resp-port "${RESP_PORT}" \
  --admin-port "${ADMIN_PORT}" \
  --kv-count 2000 \
  --vector-count 50000 \
  --graph-nodes 10000

echo "[integration] running Playwright"
export MOON_CONSOLE_URL="http://127.0.0.1:${ADMIN_PORT}/ui/"
pushd "${REPO_ROOT}/console" >/dev/null
pnpm install --frozen-lockfile
pnpm exec playwright install --with-deps chromium
pnpm test:e2e
pnpm test:integration
popd >/dev/null

echo "[integration] OK"
