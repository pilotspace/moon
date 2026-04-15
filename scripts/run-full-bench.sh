#!/usr/bin/env bash
# Full benchmark: KV + persistence + vector + graph
# Outputs to ~/bench-all.log
set -euo pipefail
cd ~/moon
source ~/.cargo/env

pkill -9 -f "moon --port" 2>/dev/null || true
pkill -9 -f redis-server 2>/dev/null || true
sleep 1
rm -rf ~/bench-results /tmp/moon-data /tmp/redis-data

echo "=== Starting full benchmark suite ==="
bash scripts/run-gcloud-bench.sh
echo "=== KV + Vector done ==="

bash scripts/bench-graph-compare.sh --skip-build --moon-only --nodes 2000
echo "=== Graph done ==="

echo "ALL_DONE"
