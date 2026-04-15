#!/usr/bin/env bash
# Setup script for GCloud benchmark instances.
# Run on each instance after SSH: bash gcloud-bench-setup.sh
set -euo pipefail

echo "=== Installing Rust 1.94.1 ==="
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1
source "$HOME/.cargo/env"
rustc --version

echo "=== Installing build dependencies ==="
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev redis-server git

echo "=== Cloning Moon ==="
if [ -d "$HOME/moon" ]; then
    cd "$HOME/moon" && git pull
else
    git clone https://github.com/pilotspace/moon.git "$HOME/moon"
    cd "$HOME/moon"
fi

echo "=== Building Moon (release, target-cpu=native) ==="
RUSTFLAGS="-C target-cpu=native" cargo build --release

echo "=== Verifying ==="
./target/release/moon --port 6399 --shards 4 &
MOON_PID=$!
sleep 2
redis-cli -p 6399 PING
kill $MOON_PID
wait $MOON_PID 2>/dev/null || true

echo "=== System Info ==="
uname -a
lscpu | grep -E "^(Architecture|Model name|CPU\(s\)|Thread)"
free -h

echo "=== Ready for benchmarks ==="
echo "Run: bash scripts/bench-compare.sh --requests 200000"
echo "Run: bash scripts/bench-production.sh"
echo "Run: bash scripts/bench-resources.sh"
