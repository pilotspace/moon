# D3 — concurrent KV keyspace (design)

**Status:** design, nothing implemented. Flag-gated and KV-only when it lands.

## Why this and not D1

The cross-shard hop costs a **fixed 13.2 µs per park** (90% CI [12.2, 14.28];
`.add/tasks/xshard-read-fastpath/d1_preflight.csv`). D1 proposed batching the
*wake* that ends the park. It is retired: monoio's `EventWaker::wake()` returns
early on `awake` — cleared only just before the driver sleeps — so under load
the eventfd write is skipped. Measured at c=200 P=1, `--shards 8`:
**0.111 write syscalls per command against 0.872 parks per command**. 87% of
cross-thread wakes are already coalesced; `--shards 1` shows 0.000.

So the fixed per-park cost is not the signal. It is the suspend/resume of the
awaiting task plus the round trip itself. The only way to remove it is to not
make the round trip. That is D3.

## What already exists

L4 landed the hard part. `Database` lives behind a per-`(shard, db)` `RwLock`
in a process-wide registry (`shard/db_plane.rs`), is `Send + Sync`, and
`try_foreign_db_read` serves a foreign **read** on the calling thread with one
CAS and no park. S3 measured taking a shared guard on the owner's *own* read
path at −0.06%, −0.41%, +1.30% across three cells — free within noise. **The
lock is not the problem.**

D3 is the same move for **writes**, plus dropping the ownership fiction for KV.

## The obstacle: a write is not just a mutation

`spsc_handler::wal_append_and_fanout` is what every write must go through. Its
parameters decide whether a foreign thread can perform a write at all:

| Side effect | Form today | Shareable? |
|---|---|---|
| WAL append | `&mut Option<WalWriterV3>` | **No** — shard-owned `&mut` |
| Replica sender fan-out | `&mut Vec<ReplicaFanout>` | **No** — shard-owned `&mut` |
| Deferred replica delivery | `self_msg::push(ReplicaLiveFanout)` | **No** — pushes to the *calling* thread's queue |
| Replication backlog | `Arc<Mutex<Option<..>>>` | Yes |
| Replication offset | `OffsetHandle`, lock-free, keyed by `shard_id` | Yes |
| AOF pool | `Arc<AofWriterPool>`, `send_append(shard_id, ..)` | Yes |

Three of six are per-shard `&mut` state, and one of those — the deferred
replica delivery — carries an **ordering guarantee**. The R2 comment is
explicit: delivery is routed through the owner's self queue so a later-offset
record cannot reach the wire before an earlier-offset one, and so a replica
registering mid-drain cannot miss a record whose offset already advanced.
A foreign thread that sends directly reintroduces exactly that bug.

This is the real cost of D3, and it is not the lock. **It is that durability
and replication are sequenced by per-shard single-threadedness.**

## Design

Execute the mutation on the calling thread under the owner's per-db write
guard; route every side effect to the owner **without a round trip**.

1. **Mutation.** `try_foreign_db_write(shard, db, f)` — mirror of
   `try_foreign_db_read`, `try_write`, never parks. On `None` (owner holds the
   guard) fall through to the SPSC path, exactly as reads do today.
2. **Ordering is bought with the guard.** Serialise record construction,
   offset assignment (`increment_shard_offset` is atomic) and enqueue **inside**
   the write guard. Two foreign writers to the same db are then ordered
   identically in the keyspace and on the wire, because the guard orders both.
   Doing any of it after the guard drops reintroduces the R2 reordering bug.
3. **WAL** goes through the existing per-shard channel
   (`SharedDatabases::wal_append(shard_id, ..)`), not the `&mut` writer.
4. **Replica fan-out** becomes a fire-and-forget message to the owner's queue
   instead of `self_msg::push`. This is still a cross-shard *message* — but not
   a *park*. The cost model is explicit that messages cost approximately
   nothing and parks cost everything; seven optimisations were already measured
   and found worthless on the opposite reading.
5. **AOF** already takes the owner's `shard_id`. No change.

## Scope

KV keyspace only. The vector, graph and text engines keep their shards — they
lean on single-owner ownership, and per-shard WAL assumes a partition.
`ShardSlice` stays `!Send`; what becomes shared is the `Database` behind the
registry, which is already `Send + Sync`.

Off by default behind its own flag, like S4. The honest cost is not throughput,
it is that shared-nothing stops being a **compile-time** guarantee for KV and
becomes a discipline.

## Staging — each stage independently revertable

- **W1** `try_foreign_db_write` primitive + ordering contract + unit tests.
  No dispatch wiring. Proves the guard orders records as claimed.
- **W2** Wire a deliberately tiny command set (single-key, no TTL) behind the
  flag. Reuse S4's gate chain verbatim — `pending_mask` (moon#507/#512),
  `single_owner_shard` (moon#592), hotness (moon#610). Those gates were each
  written for a real, measured failure.
- **W3** Durability + replication acceptance: kill -9 lossless, replica parity
  both directions, AOF/WAL attribution to the owning shard. Per the standing
  rule for replication/persistence changes.
- **W4** Measure CPU/cmd against the s8 baseline **in an unsaturated regime**.
  The c≥200 cells of the pre-flight sit at ~6.9 of 8 cores and their throughput
  ceiling; marginal-cost readings there are throughput-limited.

## Open risks

- **Multi-key atomicity** needs ordered lock acquisition to avoid deadlock.
  W2's single-key scope defers this, but MSET/MULTI cannot ship without it.
- **Blocking clients** (BLPOP waiters) live on the owner shard. A foreign write
  that satisfies a waiter must still notify the owner.
- **Expiry, eviction, notifications, tracking invalidation** are per-shard
  bookkeeping and each needs the same treatment as the WAL — enumerate the
  *state writers*, not the command names.
- **Guard contention** at depth is untested for writes. S3's "free within
  noise" result was measured for shared read guards, not exclusive ones.
