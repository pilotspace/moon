#!/bin/bash
set -u
cd /home/user/wt/int2b
export CARGO_TARGET_DIR=/home/user/wt/target-c CARGO_INCREMENTAL=0
O=${SCRATCH:-/tmp/moon-work}/gateR8
BIN=/home/user/wt/bin
HEAD=$(git rev-parse --short HEAD); grep -q "^head $HEAD" $O/status 2>/dev/null || echo "head $HEAD" > $O/status
done_(){ grep -q "^$1" $O/status; }
step(){ name=$1; shift; done_ "$name rc=0" && return; "$@" > $O/$name.log 2>&1; rc=$?; echo "$name rc=$rc $(grep -h 'test result' $O/$name.log | awk '{p+=$4;f+=$6}END{if(NR)print "passed="p" failed="f}')" >> $O/status; }
step fmt cargo fmt --check
touch src/lib.rs; step clippy_monoio cargo clippy --all-targets -- -D warnings
touch src/lib.rs; step clippy_tokio cargo clippy --all-targets --no-default-features --features runtime-tokio,jemalloc -- -D warnings
step fuzz_check cargo check --manifest-path fuzz/Cargo.toml --all-targets
touch src/lib.rs; step lib_monoio cargo test --release --lib
touch src/lib.rs; step lib_tokio cargo test --release --lib --no-default-features --features runtime-tokio,jemalloc
[ -x $BIN/i2b-$HEAD-monoio ] || { touch src/lib.rs; cargo build --release > $O/build_monoio.log 2>&1 && cp $CARGO_TARGET_DIR/release/moon $BIN/i2b-$HEAD-monoio; echo "build_monoio rc=$?" >> $O/status; }
[ -x $BIN/i2b-$HEAD-tokio ] || { touch src/lib.rs; cargo build --release --no-default-features --features runtime-tokio,jemalloc > $O/build_tokio.log 2>&1 && cp $CARGO_TARGET_DIR/release/moon $BIN/i2b-$HEAD-tokio; echo "build_tokio rc=$?" >> $O/status; }
cmp -s $BIN/i2b-$HEAD-monoio $BIN/i2b-$HEAD-tokio && echo "ALIAS: binaries identical" >> $O/status
SUITES="replica_resync_r2b4 replica_promotion_sync_r2b4 aof_boot_r2b4 embedded_layout_guard_r2b4 nondeterministic_propagation_825 stream_group_block_log_1104 stream_group_plain_log_1130 replica_blocking_wake_1096 aof_boot_damage_r2b3 promoted_replica_r2b3 cross_shard_write_barrier_1322 flat_aof_unreadable_refusal_r2b2 flat_aof_retired_by_manifest_r2b2 promoted_replica_restart_r2b2 flat_aof_snapshot_double_apply_r2b cow_stream_shutdown_1295 txn_hold_one_copy_1300 aof_auto_rewrite script_write_fsync_barrier_831 cold_cut_single_shard_914 perf_ws21_snapshot_without_save_rules replication_multishard replication_hardening txn_crash_atomicity_1300 cold_block_reclaim_no_aof_1297 cow_stream_1295 aof_shard_write_1266 volatile_ttl_eviction_order_1298 perf_ws16_bgsave_capture perf_ws21_flushall_save crash_matrix_per_shard_bgrewriteaof perf_ws12_bgsave_split replication_streaming replication_planes txn_close_after_epilogue_1299 held_release_txn_race_1289 replica_past_deadline_1286 crash_recovery_cold_del_rewrite parked_idle_parity protocol_error_lifetime subscriber_client_state blocking_peer_eof replication_ttl_semantics crash_recovery_cold_no_aof aof_fsync_stall_r1 aof_select_after_restart_r1 expired_keys_parity_1286 held_release_txn_open_1289 review_r1_txn_isolation_1299 txn_exit_epilogue_1299 acl_rule_order_1296 aof_everysec_kill9_1266 aof_replay_clock_1283 cold_held_files_release_1289 graph_wal_append_1302 info_expired_keys_1286 review_w1_txn_abort_no_aof_snapshot_1285 txn_abort_durability_1285 txn_isolation_1299 wal_group_commit
 aof_fold_exactly_once_455 aof_everysec_backpressure_769 aof_backpressure_reply_1272 aof_multidb_kill9 aof_append_status_heals_on_rewrite aof_fsync_err_subscribe_ordering aof_toplevel_multishard_refusal default_config_aof_backpressure_838 single_handler_aof_order_1099 perf_ws21_aof_drain perf_ws21_aof_writer_start perf_ws6_aof_record_alloc legacy_aof_rewrite_on_boot_914 crash_aof_init_generation_1293 crash_matrix_per_shard_aof cold_tier_aof_double_apply_902
 txn_completeness_edge_cases txn_cypher_write_rollback txn_graph_wiring txn_kv_wiring txn_multikey_undo_500 txn_partial_reject txn_partial_reject_monoio inline_read_txn_visibility_807 multi_acl_queue_time_1035 scripts_in_multi_894
 graph_wal_kv_authority_1018 crash_recovery_graph_durability graph_integration graph_restart_id_aliasing wal_kv_db_context_1039 wal_last_resort_replay_1026 wal_bounds_wired_916 wal_v3_aggressive_recycle
 active_expiry_backlog_drain_1288 tracking_expiry_invalidation_1013 acl_subcommand_rules acl_user_revocation acl_auth_surface
 cold_graves_reduced_databases_1291 cold_orphan_sweep cold_file_id_orphan_sweep_1114 crash_matrix_cold_graves_1281 tiering_no_aof_write_gate_1290 spill_thread_supervision_1265 review_r3_lua_eviction_aof eviction_reason_del_run_budget_1294 kill_snapshot"
for rt in monoio tokio; do
  feat=""; [ $rt = tokio ] && feat="--no-default-features --features runtime-tokio,jemalloc"
  ARGS=""; for t in $SUITES; do [ -f tests/$t.rs ] && ARGS="$ARGS --test $t"; done
  done_ "prebuild $rt rc=0" || { touch src/lib.rs; cargo test --release $feat $ARGS --no-run > $O/prebuild_$rt.log 2>&1; echo "prebuild $rt rc=$?" >> $O/status; }
  for t in $SUITES; do
    done_ "it $rt $t rc=" && continue
    [ -f tests/$t.rs ] || { echo "$rt $t missing" >> $O/status; continue; }
    feat=""; [ $rt = tokio ] && feat="--no-default-features --features runtime-tokio,jemalloc"
    cargo test --release $feat --test $t --no-run > $O/build_${rt}_$t.log 2>&1 || { echo "it $rt $t BUILD-FAIL" >> $O/status; continue; }
    MOON_BIN_MONOIO=$BIN/i2b-$HEAD-monoio MOON_BIN_TOKIO=$BIN/i2b-$HEAD-tokio MOON_BIN=$BIN/i2b-$HEAD-$rt MOON_DISK_FREE_MIN_PCT=0 timeout 1500 cargo test --release $feat --test $t -- --include-ignored --test-threads=1 > $O/it_${rt}_$t.log 2>&1
    echo "it $rt $t rc=$? $(grep -h 'test result' $O/it_${rt}_$t.log | tail -1 | cut -d. -f2-3)" >> $O/status
  done
done
df -h / | tail -1 >> $O/status
L=/home/user/wt/target-loom
done_ "loom rc=" || { CARGO_TARGET_DIR=$L cargo rustc --release --test loom_aof_fsync_agent -- --cfg loom > $O/loom_build.log 2>&1; echo "loom_build rc=$?" >> $O/status
LB=$(ls -t $L/release/deps/loom_aof_fsync_agent-* 2>/dev/null | grep -v '\.d$' | head -1)
[ -n "$LB" ] && { $LB > $O/loom.log 2>&1; echo "loom rc=$? $(grep -h 'test result' $O/loom.log | tail -1)" >> $O/status; }
}
rm -rf $L
for t in aof_everysec_kill9_1266 aof_shard_write_1266 aof_fsync_stall_r1; do done_ "it epoll $t rc=" && continue; MOON_NO_URING=1 MOON_BIN=$BIN/i2b-$HEAD-monoio MOON_DISK_FREE_MIN_PCT=0 timeout 2400 cargo test --release --test $t -- --include-ignored --test-threads=1 > $O/it_epoll_$t.log 2>&1; echo "it epoll $t rc=$? $(grep -h 'test result' $O/it_epoll_$t.log | tail -1 | cut -d. -f2-3)" >> $O/status; done
L=/home/user/wt/target-loom
CARGO_TARGET_DIR=$L cargo rustc --release --test loom_aof_lane -- --cfg loom > $O/loom2_build.log 2>&1; echo "loom_lane_build rc=$?" >> $O/status
LB=$(ls -t $L/release/deps/loom_aof_lane-* 2>/dev/null | grep -v '\.d$' | head -1)
[ -n "$LB" ] && { $LB > $O/loom2.log 2>&1; echo "loom_lane rc=$? $(grep -h 'test result' $O/loom2.log | tail -1)" >> $O/status; }
rm -rf $L
echo DONE >> $O/status
