# MILESTONE: Throughput Polish (recover v2-deferred costs)

goal: On monoio, FT.SEARCH's off-event-loop yield reaps the io_uring CQ without timer-wheel latency, so the brute-force chunk shrinks back to its latency-optimal size and reclaims the ~22% throughput #179 deferred, with #179's co-located latency relief preserved.
rationale: sub-milestone — a slice of the closed v2 "Multi-Core Throughput Hardening" theme. #179 shipped FT.SEARCH's latency relief but accepted a −22% brute-force QPS cost (the monoio yield uses a ~ms timer-wheel park, forcing chunk=16384). Recovering that throughput is task-sized work that touches no frozen contract, but both milestones are closed so it needs a fresh home. Scoped focused (1 task); sibling deferrals (xshard-read-coalescing C3, GCloud revalidation) left OUT.
stage: production · status: active · created: 2026-06-15

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:
- Replace monoio `cooperative_yield()`'s `sleep(ZERO)` timer-park (`src/runtime/mod.rs:50`)
  with a cost-free io_uring park-and-reap (NOP submission / already-ready self-pipe — whatever
  monoio 0.2.4 exposes safely) that still drains the task queue so the loop park()s and reaps the CQ.
- Re-tune the `max_brute_force_vecs_per_chunk` default (`holder.rs:77`, currently 16384) down to
  the new latency-optimal knee once the per-yield cost drops.
- Same-run A/B re-validation on a clean monoio VM: QPS recovery AND latency-relief preservation.

Out:
- tokio `cooperative_yield()` — already free via `yield_now`, untouched.
- `xshard-read-coalescing` (C3) and GCloud absolute revalidation — separate scope, deferred siblings.
- Any modification of #179's frozen latency contract — it is a CONSTRAINT here, never re-opened.
- HNSW / per-segment yield-point changes (only the brute-force chunk path is in scope).

## Shared decisions & glossary deltas   (living — every task must honor these)
- `cooperative_yield()` keeps its `async fn ()` signature — callers in the FT.SEARCH slices
  (`handler_monoio/ft.rs`, `handler_sharded/ft.rs`) stay unchanged; only the monoio body changes.
- No new `unsafe` without explicit user approval + a `// SAFETY:` comment (CLAUDE.md unsafe policy).
- Measurement is RELATIVE same-run A/B (absolute OrbStack RPS is untrusted); needs a verified-clean
  VM — see the leaked-busy-poller gotcha before benching cross-runtime latency.

## Shared / risky contracts (freeze these first)
- monoio yield primitive — must (a) drain the task queue so the loop parks, (b) trigger exactly one
  CQ reap, (c) re-wake promptly without ms-scale latency, (d) add no new unsafe w/o approved SAFETY.
  -> owning task `ft-yield-costfree-monoio`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [x] ft-yield-costfree-monoio   depends-on: none   — swap monoio timer-park yield → cost-free
      io_uring CQ-reap; re-tune brute-force chunk; A/B re-validate QPS recovery + latency preservation.
      DONE · gate PASS (PR #189).

## Exit criteria (observable; map each to the task that delivers it)
- [x] monoio `cooperative_yield()` no longer uses `sleep(ZERO)` — parks-and-reaps via a cost-free
      io_uring mechanism (no timer-wheel dependency), with a test.                  (← ft-yield-costfree-monoio)
      MET: `monoio_yield::park_reap` reads an always-ready `UnixStream::pair` (sleep(ZERO) is fallback-only);
      tests `monoio_yield_overhead_is_microscopic` + `monoio_yield_relieves_colocated`.
- [x] Same-run A/B on clean monoio VM: heavy brute-force FT.SEARCH QPS recovered to within ~5% of the
      timer-park-disabled / pre-#179 control, at the re-tuned small chunk.          (← ft-yield-costfree-monoio)
      MET: A/B (20k×384d, release, `tests/ft_yield_chunk_ab.rs`) K=512 = +2.74% vs sync control (within 5%, 2× margin).
- [x] Co-located p99 relief preserved — no regression beyond noise vs #179's 6.6ms(1t)/27ms(3t) anchor. (← ft-yield-costfree-monoio)
      MET: #179 relief guards (m1/m1b) stay green + relief test passes; the cost-free yield is STRICTLY finer
      (~39 yields/query at 512 vs ~1 at 16384), so relief improves, not regresses.
- [x] Both runtimes green (tokio unchanged), 0 new unsafe w/o approved SAFETY, MVCC/consistency regression green. (← ft-yield-costfree-monoio)
      MET: 3604 monoio + tokio suites green; tokio `yield_now` untouched; unsafe 218/218 (0 new); unwrap ratchet;
      clippy + fmt both runtimes.
