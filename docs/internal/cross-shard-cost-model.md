# The cross-shard cost model — what was measured, and what was disproved

Written after a multi-week investigation into why moon lost to Redis on
non-pipelined workloads. Its purpose is **negative knowledge**: seven
optimisations were measured and found worthless, and five of my own published
claims turned out to be wrong. Both lists are here so nobody spends another
afternoon re-deriving them.

Every number below comes from a Linux host — GCE `t2a-standard-8` (aarch64,
8 vCPU), dedicated load generator, one server at a time, populated keyspace.
No macOS number appears in this document, and none should be added: io_uring,
O_DIRECT, connection migration and the spin governor's `/proc` sampling are all
`cfg(target_os = "linux")`, so a native run cannot exercise them.

---

## 1. The finding: cost is per-park, not per-message

Sweeping pipeline depth and fitting per-command CPU against the two candidate
terms:

| pipeline depth | CPU%/kops | msgs/cmd | parks/cmd |
|---|---|---|---|
| p=1  | 2.55 | — | — |
| p=4  | 1.00 | — | — |
| p=8  | 0.70 | — | — |
| p=16 | 0.55 | — | — |

```
cost = 0.413 − 0.046·msgs/cmd + 2.488·parks/cmd        (CPU%/kops)
```

**A park costs 2.49 CPU%/kops ≈ 24.9 core-µs. A cross-shard message costs
approximately nothing.** At p=1 the park term is 85% of total cost.

The fit is corroborated model-free: from p=1 to p=16, messages per command fall
2.3×, parks per command fall 14×, and measured cost falls 4.6× — tracking parks,
not messages.

**This is the single most useful result in the investigation.** It reframes
every cross-shard optimisation: the question is never "can I send fewer
messages", it is "can I avoid the park". An optimisation that halves SPSC
traffic while preserving one park per command is worth ~nothing.

### Independent corroboration (PR #768)

#768 fans a spanning multi-key read into the existing slotted batch instead of
deferring the batch tail. Measured on the same rig, `--shards 4`:

| shape | control | fix | delta |
|---|---:|---:|---|
| bulk | 24,093 ops/s | 386,267 ops/s | +1503% |
| round-trip | 22,529 ops/s | 34,048 ops/s | +51.1% |

Deferrals fell 7,938 → 0 and 6,000 → 0. Every deferral is a batch boundary and
therefore a park. The commit message credits removing SPSC sends; the model says
the win is the parks, and the model is what the pipeline sweep measured
directly. Same fix, same magnitude, different mechanism — and the mechanism is
what predicts where the pattern pays next.

---

## 2. Where the time actually goes

`perf record -F 999 -g`, 8 shards, p=1:

| bucket | share |
|---|---|
| kernel (network stack) | **59.9%** |
| moon user time | 37.7% |

Splitting that user time:

| bucket | share of user time |
|---|---|
| monoio runtime | 32.6% |
| bare atomic RMW | 15.0% |
| **actual Redis work** | **16.8%** |

**83% of moon's user time is not Redis work.** Against Redis on the same host,
65% of moon's excess cost is USER time (9.5 vs 3.1 core-µs/op) — so "it's all
kernel, nothing to do" is false, even though the kernel is the single largest
bucket.

*Instrumentation note:* bucket on the DSO column (`$3`) plus the `[k]` marker,
not the thread-name column (`$2`). Getting this wrong put 51% in "other" and
made an 8-shard profile total 111%. Kernel symbols need
`sysctl kernel.kptr_restrict=0` and `perf_event_paranoid=-1`, or they resolve as
bare addresses.

---

## 3. Seven measured dead ends — do not re-propose these

| # | Idea | Result |
|---|---|---|
| 1 | Cross-connection batching | Predicted **0.99×**. Batching more work per message optimises the term measured at ~zero. |
| 2 | Remove the per-reply `Box::pin(sleep(30s))` timer | ON 2.540 vs OFF 2.545 CPU%/kops — **indistinguishable**. The timer is a monoio wheel entry, not an io_uring op; it is nearly free. |
| 3 | Extend the inline path to foreign reads | Inline forfeit is only **0.187** of commands; a perfect fix caps at **+7%**. |
| 4 | Reply-side spin | 302K → 272K → 100K → **9,303 rps** as `MAX_CONNS` rises: a **32× convoy collapse**. Bench-only, never production. |
| 5 | Shrink the connection future to help re-poll | The future is **6,208 bytes**; shrinking it changed nothing measurable. |
| 6 | Idle spin | Costs 2% CPU at **zero** clients; not a throughput lever. |
| 7 | Connection steering to co-locate keys | Information-theoretically useless for uniform keys: a connection's next key is independent of its last. |

Dead ends 1, 4 and 7 are the ones most likely to be re-invented, because each
sounds obviously correct before it is measured.

---

## 4. Five retractions — claims I published and later disproved

Recorded because each was believed, acted on, and wrong.

1. **"moon is 0.64× Redis best-vs-best."** A measurement artifact. The control
   leg drifted 43% across the run. Interleave A/B legs and establish a noise
   floor before quoting any ratio.
2. **"moon wins 1.75× at p=16."** Both servers were **client-capped** at
   1.58M rps — the benchmark, not either server, was the bottleneck.
3. **The `(N−1)/N` per-command hop model.** Refuted directly by measurement.
   Connection→shard assignment is by kernel 4-tuple hash and key→shard by
   xxh64, so they are uncorrelated — but the per-command consequence I derived
   from that did not hold.
4. **"The whole deficit is one handoff."** The inline forfeit is 10% of it.
5. **"Foreign readers are blocked by the owner's write guard."** Refuted by the
   S0b experiment (below). This was my own diagnosis and it was wrong.

---

## 5. L4 shared read plane — status

**The prototype result stands:** sharing databases so a foreign shard serves
reads locally measured **+19.4% throughput, −22.4% CPU/op**, on less total CPU.
Against Redis on the same host, with L4 on:

| workload | moon | Redis | ratio |
|---|---:|---:|---|
| uniform random | 363,192 | 317,284 | **1.14×** |
| tag co-located | 518,808 | 345,672 | **1.50×** |
| hot set | 408,582 | 365,882 | **1.12×** |

CPU per operation remains **1.31× Redis** on the uniform workload — moon wins
throughput while still costing more per op.

**S0/S0b returned NO-GO,** and this bounds what to expect from the production
version. S0b made the owner take a *shared* guard for its own reads instead of
an exclusive one, on the prototype's whole-slice lock. Throughput moved 1.018× —
nothing. So owner-write-guard reader-blocking is **not** the remaining
bottleneck, and finer lock granularity will not beat the prototype.

What survives that NO-GO, precisely:

- The production per-`(shard, db)` design is justified on **soundness**, not on
  beating the prototype. The prototype shares the whole `!Send` `ShardSlice`,
  which needs `unsafe` and exposes non-`Sync` vector/text/graph stores. It
  cannot ship in that form at all.
- The realistic target for the safe version is **prototype parity** (~367K rps),
  not better. If acceptance lands materially below that, the design is dead.
- L4-ON sits at **724% of 800%** — unsaturated, with no current hypothesis for
  what limits it. Re-profiling L4-ON is the outstanding question.

---

## 6. Measurement traps that produced wrong numbers here

Each of these silently produced a plausible, wrong result at least once:

- **Piping a gate discards its exit code.** `cargo test … | grep …` reports
  *grep's* status. Redirect to a file and capture `$?`.
- **Impossible CPU samples.** 406K rps at 1024% on an 8-vCPU host. Time the real
  window with `date +%s.%N` at both ends and discard any sample above
  `nproc × 100`; that guard then caught four more.
- **`pkill -f` matches your own session.** The ssh command line contains the
  literal path being matched. Put cleanup and launch in an on-box script.
- **`& disown` inside a compound `gcloud ssh` command silently does not launch.**
  Use a dedicated on-box wrapper script.
- **Python buffers stdout** — a healthy benchmark run looks like a hang. Use
  `python3 -u`.
- **Scraping `/metrics` costs throughput.** Enabling `--admin-port` and scraping
  depressed throughput 11% (327K vs 363K). Within-run comparisons stay valid;
  absolute and cross-run numbers do not.
- **`moon_dispatch_path_total` is not a remote-fraction metric.** The inline path
  records `path="local_inline"`, and the counter was absent on the prototype
  binary — it read a false 100%.
- **redis-benchmark 8.x emits `\r`** for progress lines: `tr '\r' '\n'` before
  grepping, and match the RPS by position — `awk '{print $2}'` yields
  `summary:`.

---

## 7. Shard-count and preset baselines

`GET`, p=1, c200, populated keyspace, one server at a time:

| config | ops/s |
|---|---:|
| `--shards 1` | 128,758 |
| `--shards 2` | 147,929 |
| `--shards 4` | 195,065 |
| `--shards 8` | 332,779 |
| Redis (`io-threads 8`) | 499,251 |

Scaling is sub-linear, as documented — cross-shard dispatch dominates the local
DashTable lookup. The gap at `--shards 8` is what L4 attacks.

Separately, `--profile standalone` as shipped from v0.7 through v0.8.7 measured
**65,156 ops/s** against stock `--shards 8`'s 332,779 — ~5× slower, the worst
row in a 30-row tuning sweep. That preset paired `--io-busy-poll-us 40` with
`--io-driver epoll`; the epoll readiness spin serialises against a single
shard's accept+read path once more than a handful of connections are live
(#772).

---

## See also

- [`env-knobs.md`](env-knobs.md) — spin governor, THP soak, io_uring dead ends.
- `src/shard/db_plane.rs` — the L4 plane's implementation and its lock rules.
