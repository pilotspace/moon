# Releases

## v0.6.0 — 2026-07-08
milestones: v0.6.0-release (PR #249, six workstreams)
waivers: shardslice-migration (expires 2026-08-01; retirement planned with v0.7 L4 cross-shard-read work)
evidence: Squash-merged 9edf7a9c (PR #249): multi-db isolation (db-scoped FT/graph/full-text indexes; per-db maxmemory quotas with truthful container-growth accounting), vector engine HOT→WARM→COLD offload (−26% RSS measured), `--profile standalone`, command-parity audit. Adversarial pre-merge reviews caught the install-time idle-clock and shrink-lockout blockers; historical WS DROP key leak fixed with operator callout in CHANGELOG. Full CI green including Windows. Ledger entry recorded 2026-07-10 (two days after ship) — the release-ledger CI gate added alongside this entry makes a tag without a ledger entry fail, so the lag cannot recur.

## v0.5.1 — 2026-07-04
milestones: none
waivers: none
evidence: recorded by add.py release

## v0.5.0 — 2026-07-04
milestones: none
waivers: shardslice-migration
evidence: Bundles v3-4 KV correctness + p=1/multi-shard throughput (--io-busy-poll-us poll-mode park, C2 convoy fix, L3b ResponseSlotPool) + vendored monoio 0.2.4 fork. Review: security supply-chain CLEAN (monoio diffed vs pristine upstream 0.2.4, 0 new unsafe); rust drop-safety found+FIXED a panic-unwind cross-shard reply UAF (Arc-owned ResponseSlotPtr, -4 unsafe blocks). Full CI green both runtimes (232 groups); GCloud A/B confirmed multi-shard c8/c64 s4 P1 win held. shardslice cross-shard-read waiver rides (validated, retirable).

## v0.4.1 — 2026-06-23
milestones: v2-2-xshard-read-validation
waivers: shardslice-migration
evidence: Measure-only validation (no src/ change): bare-metal dual-vendor GCloud absolute xshard-read validation, 4 instruments; green-pin 3/3 + harness self-test 11/11; CI green on #203 (8 success/3 skip); waiver shardslice cross-shard-read VALIDATED + retirable (follow-up cross-shard-read-acceleration closed, was expires 2026-08-01)

## v0.4.0 — 2026-06-22
milestones: v1-shared-nothing, v2-performance, v2-1-throughput-polish, v3-1-fts-hardening, v3-2-graph-correctness
waivers: shardslice-migration
evidence: Full CI matrix green on moon-dev (fmt + clippy default/tokio + test release/tokio, MOON_BIN-pinned ELF); detailed notes in CHANGELOG [Unreleased]; waiver shardslice cross-shard-read disclosed (follow-up cross-shard-read-acceleration, expires 2026-08-01)

