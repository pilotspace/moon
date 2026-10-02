# R2b3-fix-d SUMMARY

Base int-2b 1e36186, branch `w2/r2b3-fix-d`, HEAD 968c873. Binaries `r2b3fd-v5-{monoio,tokio}` at HEAD; markers "ended in a record torn by a crash", "must open with", "moon#1321). Boot this dir" in both (the `--appendfilename` refusal only in tokio — monoio ignores the option and compiles that branch out).

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| F-B MAJOR: torn tail / mid-file corruption kept the prefix and the writer appended behind it; writes acked after that boot lost at the next | FIXED | ad39bd2 | `tests/aof_boot_damage_r2b3.rs` 10 cases: red 10/10 on 1e36186 both runtimes, green on v5. Reviewer repros on v5: TT 22 keys, after-torn=acked; TK after-torn=acked; E2 refused with the byte offset (base: TT tokio lost the write / monoio refused boot 3; TK lost on both; E2 tokio served 12/20 and lost the later write) | Risks 1, 2 |
| F-C MAJOR (moon#1321): multi-shard boot over a flat appendonly.aof stored N copies | FIXED (refusal) | 5dd3ed1, 3543145 | `a_flat_aof_refuses_a_multi_shard_boot_and_migrates`: exit 2, file unchanged, no appendonlydir; the named `--migrate-aof-*` remedy then boots DBSIZE 20. Reviewer repro base 80 → refused. Unit tests in `layout_guard.rs` | embedded server not covered |
| moon#1321 other half: tokio s1 `--appendfilename foo.aof` lost every write at restart | FIXED (refusal) | 341b154 | `tokio_refuses_an_appendfilename_its_recovery_ignores` + unit; production-guide flag row says only the default name is supported | honouring the name stays open |
| F-H MINOR: tokio s1 booted empty over a monoio manifest; next monoio boot lost tokio-era keys | FIXED (refusal) | 5dd3ed1, 3543145, 968c873 | `a_monoio_manifest_refuses_a_tokio_single_shard_boot`: exit 2, AOF untouched, no flat file; remedy (BGSAVE, move appendonlydir aside) boots 30 keys. l1/l9: base lost the keys, v5 refused | — |
| F-I MINOR: restore recipe used dump.rdb | FIXED | 2c3379a | production-guide backup/restore/cron use `shard-N/shard-N.rrdshard` (`docker cp` per shard; image distroless). Recipe run on v5, both runtimes, s1 and s4, fresh dir and in place: 8/8 exact 20 keys | — |
| F-J MINOR: suites passed vacuously without MOON_BIN_MONOIO/TOKIO | FIXED | 4a8afce | `common::required_runtime_bin` panics naming what to set; applied to promoted_replica_restart_r2b2 (helper only), flat_aof_retired_by_manifest_r2b2, flat_aof_snapshot_double_apply_r2b::tokio_dir_booted_by_monoio | txn_crash_atomicity_1300 downgrade test still passes unrun without MOON_DOWNGRADE_BIN |
| nit: retire_logged "Move it aside" after a successful rename | FIXED | 116a1b5 | rename and dir fsync separate steps with their own messages | — |

Two existing tests now assert the F-H refusal: step 3 of `flat_aof_retired_by_manifest_r2b2` and one case in `aof_toplevel_multishard_refusal` (both had expected tokio to boot over a monoio manifest).

## Mechanisms
- F-B torn tail (`aof/torn_tail.rs`): when a boot will append, a torn tail is cut at the last complete record (redis `aof-load-truncated`) — flat file (`replay_aof_at_boot`, v3 and v2 recovery), monoio single-shard incr (`replay_multi_part`), every per-shard framed incr (`replay_per_shard`). Cut bytes saved first to `<file>.torn-<offset>` (never overwriting), then truncate + fsync file + fsync dir, mtime kept. A failed save/cut refuses the boot with the file unchanged. The tokio flat writer opens only after recovery (open gate); monoio writers open O_APPEND and write nothing before the listener starts. The orphan sweep never matches `.torn-` names.
- F-B corruption: mid-file corruption fails replay (unless `MOON_AOF_BEST_EFFORT_RESYNC=1`); the flat file refuses through `UnreadableAof` (exit 1, remedy: truncate a copy at the offset); manifest paths call `refuse_damaged_aof` (log, print, exit 1 at once) — returning `Err` from main used to run the orderly writer shutdown, which appended `MOON.TS … CLOSE` behind the damage. Every record must open with `*` (`ReplayChunks`, framed payloads); a damaged `*` used to parse as an inline command and replay carried on past the damage.
- F-C: `aof::layout_guard::refusal` in main.rs before any writer starts or recovery runs; streams the flat file and stops at the first data record (RDB preamble or any record other than `MOON.*`, SELECT, DEL, UNLINK), so a head-only flat file still boots `--shards N` and is retired (R2b2 F2 flow).
- F-H / appendfilename: same guard; tokio refuses `--shards 1` when a manifest exists or `--appendfilename` is not `appendonly.aof`. The unreachable tokio WARN branch removed.

## Gates (Linux container, not merge bar)
- fmt OK; clippy `--all-targets -D warnings` monoio 0, tokio 0; fuzz check 0 (`aof_incr_replay` mode 2 now replays as a boot does and asserts the cut keeps a prefix and a second boot cuts nothing; both fuzz.yml matrices already list it).
- `cargo test --release --lib -- persistence shard replication`: monoio 1684, tokio 1653, 0 failed.
- Integration (MOON_BIN / MOON_BIN_MONOIO / MOON_BIN_TOKIO = v5), monoio and tokio both green: aof_boot_damage_r2b3 10/10, flat_aof_unreadable_refusal_r2b2 2/2, flat_aof_retired_by_manifest_r2b2 2/2, flat_aof_snapshot_double_apply_r2b 8/8, crash_matrix_per_shard_aof 4/4, crash_aof_init_generation_1293 2/2, aof_replay_clock_1283 19/19, aof_multidb_kill9 4/4, aof_toplevel_multishard_refusal 2/2, cold_cut_single_shard_914 5/5, crash_recovery_cold_del_inflight_1253 3/3, crash_recovery_cold_del_resurrection 2/2, crash_recovery_cold_del_rewrite 21/21, crash_recovery_cold_multidb 1/1, crash_recovery_cold_no_aof 10/10, txn_crash_atomicity_1300 39/39 (tokio with `MOON_TEST_NO_MASTER_PSYNC=1`), promoted_replica_restart_r2b2 2/2; legacy_aof_rewrite_on_boot_914 tokio 1/1.

## Cross-ownership edits
main.rs (layout refusal call, `refuse_damaged_aof`, removed tokio WARN branch; +11 net); recovery.rs and shard/mod.rs (one call each); replay/chunks.rs (`*` rule); shard_replay.rs, shard_replay_fuzz.rs; tests/common/mod.rs (`required_runtime_bin`); promoted_replica_restart_r2b2.rs (env-guard helper only); aof_toplevel_multishard_refusal.rs and flat_aof_retired_by_manifest_r2b2.rs (expect the F-H refusal).

## Risks
1. A damaged length field that makes a later stretch look like a torn tail is cut, as redis does; the bytes are kept in the `.torn-` sidecar.
2. Mid-file corruption (including a zero-filled tail) now refuses the boot instead of serving the prefix (redis behaviour); `MOON_AOF_BEST_EFFORT_RESYNC=1` keeps the old skip.
3. Gates must set MOON_BIN_MONOIO and MOON_BIN_TOKIO for the runtime-pair suites, which now fail instead of passing unrun.
4. The embedded server does not run `layout_guard`; the legacy `server/listener.rs::run` path still boots empty on a failed AOF load (nothing in main calls it).
5. main.rs (2717) and aof/mod.rs (2189) were already over the cap; growth kept to wiring.

## CHANGELOG bullets
- Fixed — a record torn by a crash at the end of the AOF (flat file, monoio incr, per-shard incr) is cut at boot before anything is appended; the cut bytes are saved as `<file>.torn-<offset>`. Before, every write acknowledged after that boot was lost at the next (redis `aof-load-truncated`).
- Fixed — corruption in the middle of the AOF refuses the boot naming the file and byte offset, leaving the file untouched, as redis does; a record that does not open with `*` counts as corruption (it used to be read as an inline command and skipped silently).
- Fixed (moon#1321) — a flat `appendonly.aof` holding data no longer boots `--shards N` with the whole dataset copied into every shard; moon refuses and names the `--migrate-aof-*` path. tokio `--shards 1` refuses a non-default `--appendfilename`, which its recovery never read.
- Fixed — tokio `--shards 1` refuses a dir holding a monoio single-shard AOF manifest (it booted empty and the next monoio boot dropped the tokio-era writes).
- Docs — the backup/restore recipe uses the per-shard `shard-N/shard-N.rrdshard` snapshot files; verified on both runtimes at 1 and 4 shards.

## Self-evaluation (0–1)
Completeness 0.92 · Clarity 0.9 · Practicality 0.92 · Optimization 0.9 · Edge cases 0.9 · Self-evaluation 0.9
