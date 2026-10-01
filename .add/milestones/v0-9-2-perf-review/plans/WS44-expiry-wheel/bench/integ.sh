#!/bin/bash
# usage: integ.sh LABEL BIN WHEEL
cd /home/user/wt/lane-c
export CARGO_TARGET_DIR=/home/user/wt/target-c CARGO_INCREMENTAL=0
LABEL=$1; BIN=$2; W=$3
SUITES="active_expiry_backlog_drain_1288 info_expired_keys_1286 expired_keys_parity_1286 tracking_expiry_invalidation_1013 eviction_reason_del_run_budget_1294 cold_tier_observability expiry_wheel_volatile_ttl_1298 txn_isolation_1299 review_r1_txn_isolation_1299 replication_swapdb replication_ttl_semantics"
ARGS=""; for s in $SUITES; do ARGS="$ARGS --test $s"; done
echo "=== $LABEL wheel=$W bin=$BIN"
MOON_EXPIRY_WHEEL=$W MOON_BIN=$BIN MOON_DISK_FREE_MIN_PCT=0 timeout 3000 cargo test $ARGS --no-fail-fast -- --include-ignored --test-threads 1 2>&1 | grep -E "^test .*(FAILED|failed)|^test result|Running|panicked at" 
echo "=== done $LABEL wheel=$W"
