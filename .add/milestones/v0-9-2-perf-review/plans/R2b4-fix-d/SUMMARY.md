# R2b4-fix-d SUMMARY (R2b round 4, lane D)

Branch `w2/r2b4-fix-d`, 10 commits on int-2b 5238df3. Binaries `r2b4fd-v1-{monoio,tokio}`; markers "holds BOTH an AOF manifest", "damage inside the file", "exceeds the replay parser's limits" present (the PITR refusal string is absent — that path is dead in the binary, see F9).

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| F1 migrate remedy unsafe | FIXED (full rewrite) | e5e1af0 | `--migrate-aof-*` replays the whole source into one 16-db keyspace, then splits it by shard; builds in `<to>/.migrate-staging`, published with one rename + fsync, staging removed on any error. Refuses corruption, a per-shard source, cold-tier (`MOON.SPILLED`) records, an existing target manifest, `to == from`. Reviewer's mig.sh, 8 combos (tokio/monoio × kill/rw × with/without extra dbs): refused at s4, migrates rc=0, new dir compares SAME (336/314/671/628 keys). Unit tests: multi-key, SELECT, XGROUP, unterminated TXN block, RDB preamble, damaged source, cold keys, empty source, single-shard manifest source | moon#1326: cold-tier source, per-shard → other shard count, memory (whole dataset in RAM) |
| F2 uncapped replay / SPOP | FIXED | 03be070 | All replay readers use `log_parse_config()` (no element cap; bulk and depth caps kept; validate before allocate). SPOP and XCLAIM/XREADGROUP effects chunked at 1024. Limit hit → `AofError::RecordTooLarge`, no truncate advice. Repro (SADD 1.1M, SPOP 1,060,000, SET after, kill -9, restart) monoio/tokio × s1/s4: `scard 40000 after=1`; base refuses. Fuzz crate builds | — |
| F4 ENOSPC sidecar | FIXED | fb0806d | On a full disk the torn tail is truncated without a sidecar; log gives offset, length, 64-byte hex prefix; a partial sidecar is always deleted (redis `aof-load-truncated` parity; the cut bytes are a torn, never-acknowledged record). tmpfs repro (tokio flat, monoio incr): boots dbsize 12, 0 sidecars, later write survives restart; base refuses | — |
| F5 CLOSE behind torn bytes on a failed boot | FIXED | db5b0f4, e6b6bc1 | `BOOT_PENDING` suppresses the clean-close record until the boot completes. Repro (torn tail + `shard-0/data` regular file): the failed boot appends nothing (manifest layouts byte-identical; tokio flat only cut with sidecar); a good boot keeps all 10 keys + later writes, s1/s4 both runtimes; base fails | — |
| F6 manifest + flat both hold data | FIXED | f449a06 | Refused exit 2, message contains "BOTH" with recovery steps (keep one, or merge with DUMP/RESTORE); nothing retired. fhmix: both runtimes refuse, files untouched (base monoio booted dbsize=1 and silently retired the flat file) | — |
| F7 embedded guard | FIXED | b514a12 | `embedded.rs` guard call before recovery; tokio test `embedded_layout_guard_r2b4` 2/2 (flat data at --shards 4 refused, file untouched; `--appendfilename foo.aof` refused) | — |
| F8 F-H message for multi-shard manifests | FIXED | f449a06 | fh4 (monoio s4 per-shard manifest) under tokio s1: "shard count changed (manifest=4, config=1)" | — |
| F9 PITR skips and never cuts the AOF | FIXED (refuse) | 4e793eb | `KvSources::for_target` refuses a recovery target when `appendonly.aof` holds records, before anything is read or written (a cut alone would be undone by the next non-target boot). Unit-tested | Finding: the binary never passes `--recovery-target-*` into recovery (both callers pass `None`; flags parsed and ignored) — library API only; the silent no-op flags need an issue |
| F10 messages | FIXED | 90aed3b | Refusals split by cause: read failure (EMPTY-dataset wording true there); mid-file damage (prefix readable; truncating DROPS every later record); torn tail not cut (`AofError::TornTailCutFailed`, file unchanged); limit. Manifest replay errors read "AOF replay failed". redis `#TS:` and other `#` lines at a record boundary skipped like redis: reviewer's d5_ts.aof and a fresh `aof-timestamp-enabled` AOF load SAME as redis 7.2.7 (21 and 2 keys); base refuses both | — |
| F11 backup recipe | FIXED | d31bbd8 | Script: checks the BGSAVE reply, waits on `rdb_bgsave_in_progress` with a timeout, requires `rdb_last_bgsave_status:ok`, non-zero exit otherwise; cron runs the script. Real runs: tokio and monoio s4 rc 0, 4 files; BGSAVE failing on a 1 MB tmpfs → "BGSAVE failed", rc 1 | — |
| F3 MULTI/EXEC markers | out of scope | — | — | moon#1325 (user decision) |

## Gates (Linux container, not merge bar)
- fmt; clippy `-D warnings --all-targets` monoio and tokio,jemalloc; fuzz check: pass.
- Lib (`persistence replication shard migrate error`, + `server::embedded` on tokio): monoio 1865, tokio 1818, 0 failed.
- Integration (MOON_BIN / MOON_BIN_MONOIO / MOON_BIN_TOKIO pinned, `--include-ignored`), monoio all 15 pass: aof_boot_r2b4 (5), aof_boot_damage_r2b3 (10), flat_aof_unreadable_refusal_r2b2, flat_aof_retired_by_manifest_r2b2, flat_aof_snapshot_double_apply_r2b (8), aof_toplevel_multishard_refusal, crash_matrix_per_shard_aof, crash_aof_init_generation_1293, aof_replay_clock_1283 (19), aof_multidb_kill9, nondeterministic_propagation_825, stream_group_block_log_1104, stream_group_plain_log_1130, replica_blocking_wake_1096, replication_streaming (7). Tokio: same, except replica_blocking_wake_1096 (2), stream_group_block_log_1104 (1), stream_group_plain_log_1130 (1) — "replica link never came up", identical on base i2b-5238df3-tokio (no master-side PSYNC on tokio).
- RED: aof_boot_r2b4 fails 5/5 on base binaries of both runtimes, each for its reason.

## Cross-ownership edits
embedded.rs (guard call only); replication/effect_rewrite.rs and stream_effect.rs (effect chunking); main.rs +9 net (boot_started/boot_completed, manifest layout to the guard, refusal prefixes); shard/mod.rs and recovery.rs (`UnreadableAof::from_error`, 2 lines for F9); docs/production-guide.md.

## Risks
- shard_replay.rs at 1469 lines (cap 1500).
- `#`-line skip: a damaged file whose next bytes happen to be a valid array replays past one dropped line (redis behaviour).
- ENOSPC: the torn bytes are lost except the 64-byte hex prefix in the log (deliberate).
- Migrate holds the whole dataset in memory (moon#1326).
- No fuzz campaign run; the `#` branch is reachable by `aof_incr_replay`.

## CHANGELOG bullets
- AOF replay has no per-record element cap, and SPOP/XREADGROUP/XCLAIM effects are logged in chunks of 1024: a restart after `SPOP key 1060000` no longer refuses its AOF; a record over a parser limit is reported as such, never as corruption to truncate.
- `--migrate-aof-*` replays the source into one keyspace then partitions it (multi-key commands, SELECT, stream groups and RDB preambles migrate exactly), writes into a staging dir published with one rename, and refuses damaged, cold-tier and per-shard sources.
- A torn AOF tail on a full disk is cut without a sidecar (offset, length and hex prefix logged); a partial sidecar is never left behind.
- A boot that fails before its AOF replay no longer appends a close marker behind a torn tail.
- A data dir whose AOF manifest and `appendonly.aof` both hold data is refused (exit 2) instead of silently retiring the file; the tokio single-shard refusal applies only to a single-shard manifest.
- The embedded server applies the AOF layout refusals before recovery.
- A point-in-time recovery target refuses a flat AOF that holds records.
- AOF boot refusals name their real cause and remedy; manifest replay errors no longer read "AOF rewrite failed"; redis `#TS:` annotation lines are skipped as redis does.
- docs: the backup recipe waits for BGSAVE with a timeout and checks `rdb_last_bgsave_status`.

## Self-evaluation (0–1)
Completeness 0.93 · Clarity 0.92 · Practicality 0.92 · Optimization 0.90 · Edge cases 0.91 · Self-evaluation 0.91
