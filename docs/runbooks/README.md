# Operator Runbooks

Concrete, step-by-step incident response procedures for operating Moon.

| Runbook | When to reach for it |
|---|---|
| [shard-count-change.md](shard-count-change.md) | Startup refusal: `ERR shard count changed (manifest=N, config=M)` |
| [corrupted-aof-recovery.md](corrupted-aof-recovery.md) | AOF corruption — partial replay and recovery fallback |
| [disk-full-during-wal-rotation.md](disk-full-during-wal-rotation.md) | Persistence hits ENOSPC mid-rotation |
| [oom-during-snapshot.md](oom-during-snapshot.md) | Memory pressure during BGSAVE |
| [replica-fell-behind.md](replica-fell-behind.md) | Diagnosing and remediating replication lag |
| [rolling-restart.md](rolling-restart.md) | Graceful drain + binary swap under load |
| [multi-shard-aof-rewrite.md](multi-shard-aof-rewrite.md) | Per-shard BGREWRITEAOF operations |
| [tls-cert-rotation.md](tls-cert-rotation.md) | Zero-downtime certificate rotation via SIGHUP |

Read alongside the [Production Contract](../PRODUCTION-CONTRACT.md)
(durability and availability guarantees) and the
[Operator Guide](../OPERATOR-GUIDE.md) (memory accounting, sizing).
