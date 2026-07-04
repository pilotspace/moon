# MILESTONE: Write-Path Durability & Volume

goal: Moon's write path loses no client-acknowledged write silently, writes each KV record to disk exactly once unless a WAL consumer needs it, and appendfsync=always local legs ride group commit instead of serial fsync
rationale: sub-milestone — 2026-07-04 WAL/AOF write-path investigation (tmp/WAL-AOF-REVIEW.md, building on tmp/WRITE-DIAG.md byte-level attribution) confirmed three independent, red/green-testable defects sharing the persistence writer machinery: (1) everysec backpressure silently drops acknowledged writes (+OK for a write that never reached the writer — design-for-failure violation), (2) shards≥2 + appendonly=yes double-writes every KV record to AOF **and** WAL even though Phase-B recovery discards the WAL copy (2.7× file-byte / 4.1× device amplification, measured), (3) 0.5.0's persist_local_leg does serial per-write fsync outside group commit (the measured 2000–3000ms always-pipeline tail; carried [HIGH] from v3-4). Too coupled for three orphan tasks, too small for new-major. ⚠ INTAKE UNCONFIRMED — created in auto mode while the human was away (AskUserQuestion timed out); confirm or re-size before starting tasks.
stage: production · status: active · created: 2026-07-04

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:
- **everysec fail-loud:** `AofWriterPool::try_send_append` (pool.rs:404-419) drops the append on
  channel-full/disconnected yet `try_send_append_durable` still returns `Ok(())` → client sees `+OK`
  for a write that never reached the writer thread. Replace silent drop with an error frame or bounded
  backpressure; `AOF_BACKPRESSURE_DROPPED` counter stays as the observability hook.
- **WAL KV consumer gate:** at shards≥2 + appendonly=yes every SPSC-dispatched write is logged to BOTH
  the per-shard AOF and the per-shard WAL (`wal_append_and_fanout`, spsc_handler.rs:2438-2499; channel
  wired at event_loop.rs:456-459 on bare `appendonly=yes`), while Phase-B recovery (main.rs:1052+)
  `db.clear()`s the WAL-replayed state and replays the AOF. Gate WAL KV logging behind actual consumers
  (CDC registry / PITR / disk-offload cold tier) → ~44% write-volume cut, zero crash-recovery loss.
  Must include red/green crash-recovery proof: shards≥2 kill -9 → full recovery from AOF alone.
- **local-leg group commit:** `persist_local_leg` (coordinator.rs:244-263) awaits a per-write fsync ack
  under appendfsync=always — serial fsync outside the group-commit path shipped in 0.5.0 for the writer
  loops. Route it through group-commit/`fsync_barrier` so pipelined always-tail is batch-bounded, not
  2000ms-timeout-stacked.
Out (deferred, tracked as deltas — not this milestone):
- Hot-path `Bytes::copy_from_slice` per command in `wal_append_and_fanout` (spsc_handler.rs:2497) +
  single-pass encode for both logs (the everysec P64 2.5× encode tax) — needs GCloud magnitude first.
- WAL-v3 `always` fsync inline on the shard event-loop thread (event_loop.rs:1392-1398) — whole-shard
  stall; only bites under always+disk-offload; measure before moving off-thread.
- GCloud real-SSD magnitude run (OrbStack fsync is near-free; all fsync-bound numbers are lower bounds).
- Decision record: `disk_offload=yes && appendonly=yes` Phase B silently discards v3 checkpoint-recovered
  state in favor of AOF replay — intentional? (structural interaction, undocumented).
- Coordinator `BITOP`/`COPY`/`DEL`/`UNLINK` local-leg durability gap (carried from v3-4).

## Shared decisions & glossary deltas   (living — every task must honor these)
- **AOF stays the crash-recovery authority.** No task may make WAL-v3 authoritative; the fix direction is
  always "stop writing what recovery discards", never "drop the AOF".
- **Fail-loud floor:** a client `+OK` implies the write reached the durability machinery under the active
  fsync policy's contract. No new silent-drop path may be introduced (design-for-failure rule).
- **No new hot-path allocation** in dispatch/SPSC paths while touching `wal_append_and_fanout`.
- Volume ≠ throughput: at shards≥2 throughput is dispatch-bound (3.3×), not disk-bound — task perf claims
  must be framed as write-volume / tail-latency / disk-wear wins unless GCloud-measured otherwise.

## Shared / risky contracts (freeze these first)
- WAL-consumer predicate (when is the per-shard WAL KV channel wired) -> owning task wal-kv-consumer-gate
- `try_send_append_durable` return contract under backpressure per fsync policy -> owning task aof-everysec-fail-loud

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [ ] aof-everysec-fail-loud    depends-on: none — everysec/no backpressure returns an error (or applies bounded backpressure) instead of dropping + `+OK`; red test = fill the 10k channel, assert no silent loss
- [ ] wal-kv-consumer-gate      depends-on: aof-everysec-fail-loud (shared pool contract) — WAL KV logging only when CDC/PITR/cold-tier consumes it; byte-attribution proves single-write at shards≥2; crash-recovery red/green from AOF alone on both runtimes
- [ ] local-leg-group-commit    depends-on: aof-everysec-fail-loud (shared pool contract) — persist_local_leg rides group-commit/fsync_barrier; always-pipeline tail bounded by batch fsync, not serial 2000ms awaits

## Exit criteria (observable; map each to the task that delivers it)
- [ ] A write acknowledged `+OK` under everysec is never silently dropped on writer backpressure — fill-channel test proves error-or-block, both runtimes        (← aof-everysec-fail-loud)
- [ ] shards≥2 + appendonly=yes + no WAL consumer writes ~1× the RESP bytes to disk (scripts/diag-write-attribution.sh shows WAL-v3 at header-only), and kill -9 recovery restores every acknowledged write from the AOF alone        (← wal-kv-consumer-gate)
- [ ] appendfsync=always pipelined SET tail no longer exhibits the 2000ms fsync-await stacking (GCloud A/B vs 0.5.0 baseline, same instance)        (← local-leg-group-commit)
- [ ] Consistency suite (1/4/12 shards) + full CI matrix (both runtimes) green — no durability regression        (← all)

## Close — ship review   (AI fills when every task is done — the evidence behind the engine gate, read before the boxes are checked)
> Whole-milestone, cross-task review the AI fills in. It is the evidence behind the EXISTING engine
> gate (milestone-done / checking the Exit-criteria boxes) — NOT a new approval. Tool-agnostic.

### Ship by domain   (what changed, per bounded context)
- tooling : <add.py / state.json / templates — what shipped, or "untouched">
- skill   : <SKILL.md / phases/* / guides — what shipped, or "untouched">
- book    : <docs/* — what shipped, or "untouched">

### Cross-task evidence   (one row per task)
- <slug> : gate=<PASS|RISK-ACCEPTED> · tests=<n green> · residue=<none|note>

### Goal met?   (map the evidence back to this milestone's Exit criteria — read before the Exit-criteria boxes are checked)
- [ ] each Exit criterion above is satisfied by a Cross-task evidence row or a Ship-by-domain change (cite which)
- goal: <restate the milestone goal — and the one evidence line that proves the ship meets it>

## Release steps   (AI-DEFINED — fill the ordered steps to ship this milestone; engine records, human gate)
> The AI writes the release steps for THIS milestone here (hints, not engine commands). MERGE is one
> small step among them. These feed the release scope (release.md) when the cut is bundled.
- [ ] <step — e.g. open a PR from the Close ship-review above; the human reviews + merges>
- [ ] <step — e.g. export the ship-review to a hand-off doc, e.g. `pandoc CLOSE.md -o close.docx`>
- [ ] <step — e.g. tag / publish / deploy  (human-run, per release.md)>
