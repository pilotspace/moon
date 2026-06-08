---
title: "Change Data Capture (CDC)"
description: "Stream Moon WAL records as Debezium-compatible JSON envelopes."
---

# Change Data Capture (CDC)

Moon v0.2 exposes its per-shard WAL as a structured change stream. Records are
translated into typed events and emitted as **Debezium-compatible JSON
envelopes** so they plug into Kafka Connect, Flink CDC, and the wider
ETL/streaming ecosystem with no custom decoder.

> **Status (v0.2.0-alpha).** v1 ships a **polling** command (`CDC.READ`).
> Push-based streaming over RESP3 Push frames (`CDC.SUBSCRIBE`) is on the v0.2
> roadmap (C3b) — consumer-side envelope shape is identical, so callers can
> switch later without re-parsing.

## `CDC.READ`

```
CDC.READ <wal_dir> <from_lsn> [LIMIT N]
```

Drains a batch of WAL records and returns them as Debezium envelopes.

| Arg | Type | Notes |
|-----|------|-------|
| `wal_dir` | string | Absolute path to the shard WAL directory. |
| `from_lsn` | `u64` | First LSN to include (records with `lsn < from_lsn` are skipped). |
| `LIMIT N` | `u64` | Optional cap. Defaults to **256**, hard ceiling **10 000**. |

### Response shape

```text
*<N+1>
  :<next_lsn>          # cursor — pass back as <from_lsn> on next poll
  $...{"op":"u",...}   # envelope 1 (lowest LSN)
  $...{"op":"u",...}   # envelope 2
  ...
```

- First frame is **always** an integer cursor.
- When no records are available, the response is `[from_lsn]` (length 1) — a
  stable "no new data" signal the consumer can detect without parsing
  timestamps.
- Envelopes are returned **in LSN order**.

### Polling loop (pseudo-code)

```python
cursor = 1
while True:
    arr = client.execute("CDC.READ", "/var/lib/moon/wal-0", cursor, "LIMIT", "1000")
    cursor = arr[0]
    for env in arr[1:]:
        handle(json.loads(env))
    if len(arr) == 1:
        time.sleep(0.1)   # idle backoff
```

Each call opens a fresh `WalTailReader`. Use `LIMIT >= 100` to amortize the
per-poll filesystem walk; the default of 256 is sized to fit comfortably in a
1 MB RESP response.

## Envelope schema

Moon emits a payload-only Debezium envelope. Schemas are omitted in v0.2 — the
key/value bodies are self-describing JSON.

```json
{
  "op": "u",
  "ts_ms": 1735689600000,
  "source": {
    "version": "moon-0.2",
    "connector": "moon",
    "shard": 0,
    "lsn": 12345
  },
  "before": null,
  "after": {
    "record_type": "kv",
    "command": "SET",
    "key": "users:42:name"
  }
}
```

| `op` | Meaning |
|------|---------|
| `c` | Insert (reserved for future use). |
| `u` | Upsert (`SET`, `HSET`, `HMSET`, `XADD`, vector/graph upserts). |
| `d` | Delete (`DEL`, `UNLINK`, `HDEL`, `XDEL`). |
| `r` | Read snapshot record (reserved). |

Binary keys that are not valid UTF-8 are encoded as `{"_b64":"…"}` instead of a
JSON string. This is the same fallback Debezium uses for byte-typed columns.

### Record types

| `record_type` | Source | Example WAL records |
|---------------|--------|---------------------|
| `kv` | Redis-style command | `SET`, `DEL`, `HSET`, `XADD` |
| `temporal` | Bitemporal upsert | `TemporalUpsert` |
| `graph` | Graph mutation | `GraphTemporal` |
| `checkpoint` | WAL checkpoint marker | `Checkpoint` |

## Rust API

The same translation pipeline is exposed as a library for in-process consumers
(replication probes, embedded analytics, custom sinks):

```rust
use moon::cdc::{decode_wal_record, encode_debezium};
use moon::persistence::wal_v3::{TailCursor, WalTailReader};

let mut tail = WalTailReader::new(wal_dir, TailCursor::start());
while let Some(rec) = tail.read_next()? {
    let event = decode_wal_record(&rec, /* shard = */ 0);
    let json  = encode_debezium(&event, now_ms());
    sink.send(json).await?;
}
```

`WalTailReader` is re-read-safe (it re-stats `flush_offset` on every call) and
auto-advances across segment rotations.

## Operational notes

- **Per-shard scope.** Each shard has its own WAL and its own LSN space.
  Multi-shard consumers must poll each `wal_dir` independently and merge
  client-side. Cross-shard global ordering is deferred to v0.3.
- **Retention.** CDC consumers depend on WAL segments still being on disk.
  Tune `--wal-retention-bytes` and the snapshot cadence so the slowest
  consumer's `from_lsn` is always covered.
- **Throughput.** Polling overhead is dominated by the per-call segment scan.
  Larger `LIMIT` amortizes this; sustain rate is bounded by WAL write
  throughput (~100K events/s/shard in benchmarks).

## Verification

Unit tests in `src/command/cdc/read.rs` cover:

- `test_cdc_read_drains_in_lsn_order` — happy-path 10-record batch.
- `test_cdc_read_respects_from_lsn` — `from_lsn` filters strictly below.
- `test_cdc_read_no_new_records_returns_cursor_only` — len == 1 idle signal.
- `test_cdc_read_limit_clamps_batch` — `LIMIT N` cap.
- `test_cdc_read_argument_errors` — friendly RESP errors on arity/keyword
  mistakes.

## Out of scope (v0.2.0-alpha)

- ❌ Push streaming (`CDC.SUBSCRIBE` over RESP3 Push) — v0.2 follow-up.
- ❌ Avro / Protobuf envelopes — JSON only.
- ❌ Schema-aware DDL events — Moon has no DDL.
- ❌ Cross-shard transactional ordering — per-shard streams only.
- ❌ Avro schema registry integration.
