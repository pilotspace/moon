# MILESTONE: V2 2 Xshard Read Validation

goal: the shipped cross-shard read fast-path is validated on a trustworthy absolute instrument (GCloud bare-metal), with a data-backed decision on whether coalescing / RCU acceleration is warranted
rationale: the xshard-read-fastpath win (+18→+22.9% c1 GET) was only ever a SAME-RUN RELATIVE ratio on OrbStack — an instrument proven invalid for ABSOLUTE cross-shard latency (host→guest vCPU starvation inflated 36µs→580µs). The shardslice waiver (expires 2026-08-01) deferred the absolute validation. Billing reopened → close the residue with real bare-metal numbers.
stage: production · status: active · created: 2026-06-22

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`.

## Scope
In:  port the quiesced-xshard instrument to GCloud bare-metal; record ABSOLUTE s4-c1-GET (+ guard cells) across the 4 reference commits {3e376a1, a497602, 7048e8a, 7e0a5db} on both runtimes; emit ONE data-backed disposition (close-line / build-coalescing / reopen-RCU) with the residual gap quantified in absolute µs.
Out: any `src/` change (MEASURE-ONLY — a code follow-on is a separate task behind its own contract); building the coalescing path; reopening RCU/snapshot; re-tuning the C2 gate.

## Shared decisions & glossary deltas   (living — every task must honor these)
- MEASURE-ONLY boundary (user-confirmed 2026-06-22): zero `src/` edits; verification is EVIDENCE-gated, not a code change.
- ABSOLUTE numbers come ONLY from GCloud bare-metal (CLAUDE.md) — the OrbStack VM is invalid for absolute cross-shard latency; relative ratios there are NOT absolute evidence.
- Instrument validity is FAIL-CLOSED: a contended/dirty/single-rep instrument VOIDs the cell (load/steal/clean/s1-LOCAL-control gates) — never reported.

## Shared / risky contracts (freeze these first)
- the GCloud-xshard absolute harness interface + evidence-table schema + recommendation artifact -> owning task `xshard-read-gcloud-validation` (FROZEN@v2, dual-vendor amendment).

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [x] xshard-read-gcloud-validation   depends-on: none   — port instrument to GCloud bare-metal, record the absolute table (4 commits × 2 runtimes × 2 vendors), emit the disposition.

## Exit criteria (observable; map each to the task that delivers it)
- [x] A reusable GCloud-xshard absolute harness exists AND an evidence table reports absolute s4-c1-GET µs/RPS for all 4 commits × both runtimes (× 2 vendors), with the C2 recovery and residual-to-lock-read-floor in absolute µs   (← xshard-read-gcloud-validation: `scripts/gcloud-xshard-absolute.sh` + `tmp/XSHARD-GCLOUD-ABS.md`)
- [x] Guard cells (c100, P16) recorded unregressed at the C2 commit; s1-LOCAL control confirms instrument validity   (← xshard-read-gcloud-validation: M3 tables, `control_flat: OK` on both runs)
- [x] Exactly ONE disposition recommended with the numbers behind it; the `CoalescedReadBatch` scaffolding's fate named   (← xshard-read-gcloud-validation: C4 = close-the-line, coalesced_read_batch_fate=remove-scaffolding)
- [x] `src/` untouched; C2 gate constants unchanged; the shardslice waiver's absolute-validation residue closed   (← xshard-read-gcloud-validation: `git diff --stat src/` empty + green-pin 3/3; SPEC delta folds the waiver residue)

## Close — ship review

### Ship by domain   (what changed, per bounded context)
- tooling : ADD engine untouched (one [ADD·open] delta logged: `_scope_walk` md5's `target/` → hangs; scope token-resolver is position-sensitive for root files).
- skill   : untouched.
- book    : untouched.
- moon src: UNTOUCHED (measure-only — the whole point). Deliverable is `scripts/gcloud-xshard-absolute.sh` (harness) + `tests/xshard_mechanism_unchanged.rs` (green-pin) + `tests/xshard_gcloud_harness_selftest.sh` (gate self-test) + `tmp/XSHARD-GCLOUD-ABS.md` (evidence + recommendation) + `CHANGELOG.md` [Unreleased] entry.

### Cross-task evidence   (one row per task)
- xshard-read-gcloud-validation : gate=PASS · tests=3 green-pin (release) + 11 self-test gates (fail-closed) · residue=2 seeded follow-ups (remove dead CoalescedReadBatch scaffolding; RCU explicitly dropped) — none blocking.

### Goal met?
- [x] each Exit criterion above is satisfied by a Cross-task evidence row or a Ship-by-domain change (cited inline per criterion)
- goal: the shipped C2 cross-shard read fast-path is VALIDATED ABSOLUTE on bare-metal — 4 instruments (Intel c3 + AMD c2d × monoio + tokio) agree: C2 recovers 38–49% of the SPSC regression, leaving a vendor/runtime-INVARIANT ~10µs structural residual (one irreducible cross-thread reply hop). Decision: **close-the-line** — neither coalescing (can't help the singleton; concurrent guard already flat) nor RCU (RSS cost unjustified by 10µs) is warranted. The one evidence line: `tmp/XSHARD-GCLOUD-ABS.md` C3 table + C4 recommendation.

## Release steps   (AI-DEFINED — fill the ordered steps to ship this milestone; engine records, human gate)
- [ ] commit the deliverable on a feature branch (harness + 2 tests + evidence doc + CHANGELOG entry); `src/` stays clean — CONFIRM `git diff --stat src/` empty in the commit.
- [ ] human reviews `tmp/XSHARD-GCLOUD-ABS.md` (the recommendation) + the §6 GATE RECORD; approve or request changes.
- [ ] open a PR (human-gated per CLAUDE.md; note: the instance `gh` account is pull-only — open from the macOS host) with the CHANGELOG [Unreleased] entry riding along (CI-Lint blocker).
- [ ] on merge: the [SPEC·seeded] "remove CoalescedReadBatch scaffolding" follow-up may be picked up as a tiny src-touching task; the shardslice waiver (expires 2026-08-01) can be retired as validated.
