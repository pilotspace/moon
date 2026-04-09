# Operator Runbooks

This directory will hold Moon's operator runbooks — concrete, step-by-step incident response procedures.

**Status:** Placeholder. Runbooks are authored in Phase 99 (`REL-05` of the v0.1.3 Production Readiness milestone).

**Planned runbooks:**

- `replica-fell-behind.md` — diagnosing and remediating replication lag
- `disk-full-during-wal-rotation.md` — recovery when persistence hits ENOSPC
- `oom-during-snapshot.md` — handling memory pressure mid-BGSAVE
- `corrupted-aof-recovery.md` — partial-replay and recovery v3 fallback
- `tls-cert-rotation.md` — zero-downtime certificate rotation via SIGHUP
- `rolling-restart-under-load.md` — graceful drain + binary swap procedure

Until Phase 99 lands these documents, refer to the [Production Contract](../PRODUCTION-CONTRACT.md) for durability and availability guarantees, and the [CHANGELOG](../../CHANGELOG.md) for recent operational changes.
