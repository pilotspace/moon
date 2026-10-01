WS44 (moon#1298) A/B tooling. Every script runs ONE server per row on a fresh --shards 1 instance,
MOON_DISK_FREE_MIN_PCT=0, appendonly off, and takes MOON_EXPIRY_WHEEL=0|1 for the switch.

memtest.py BIN PORT CASE N [MOON_EXPIRY_WHEEL=1]
    CASE: notl | ttl | notl_long | ttl_long | ttl_sparse | ttl_sparse_long.  Needs a
    `--features jemalloc-stats` binary for the allocator_* columns (used_memory excludes the index).
ab_set2.sh PORT REPS N KEYSPACE label=BIN:WHEEL ...
    SET key:<rand> v EX 3600 at P1 and P16, c50; prints client rps AND server CPU us/op
    (from /proc, robust to a shared box). Order alternates per rep.
drain.py BIN PORT N [MOON_EXPIRY_WHEEL=1]   (TTL_MS=3000 for the moon#1288-like dense backlog)
integ.sh LABEL BIN WHEEL   the integration gate list, one runtime x one switch value.
In-process (no server): cargo test --profile release-fast --lib expiry_wheel_tests::bench_index_ab \
    -- --ignored --nocapture --test-threads 1 ; and probe_one_cell under valgrind --tool=callgrind.
