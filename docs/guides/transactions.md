---
title: "Cross-store transactions"
description: "Atomic writes across KV, vector, and graph stores with TXN.BEGIN/COMMIT/ABORT."
---

# Cross-store transactions

Moon provides cross-store ACID transactions via `TXN.BEGIN`, `TXN.COMMIT`, and `TXN.ABORT`. These enable atomic writes that span KV operations, vector auto-indexing, graph mutations, and message queue enqueues — all committed or rolled back as a unit.

!!! warning
    Cross-store transactions (`TXN.*`) cannot be mixed with Redis `MULTI`/`EXEC` blocks. Starting a `TXN` while in a `MULTI` block (or vice versa) returns an error.

## Quick start

```bash
redis-cli -p 6379

# Start a transaction
127.0.0.1:6379> TXN BEGIN
OK

# KV writes are buffered in write intents
127.0.0.1:6379> SET user:1 alice
OK

# Vector auto-index is deferred until commit
127.0.0.1:6379> HSET doc:1 title "Hello" vec <vector_bytes>
(integer) 2

# Graph mutations are recorded as intents
127.0.0.1:6379> GRAPH.ADDNODE social alice Person '{"name":"Alice"}'
(integer) 1

# Commit all changes atomically
127.0.0.1:6379> TXN COMMIT
OK
```

## Commands

| Command | Description |
|---------|-------------|
| `TXN BEGIN` | Start a new cross-store transaction. Returns `+OK` or error if already in a transaction |
| `TXN COMMIT` | Commit all buffered changes atomically. Writes a WAL record for crash recovery |
| `TXN ABORT` | Roll back all changes via undo-log replay. Releases all write intents |

## How it works

1. **TXN BEGIN** creates a `CrossStoreTxn` on the connection, initializing an undo log and intent buffers for KV, vector, and graph stores.

2. **During the transaction**, writes are intercepted:
   - **KV writes**: Applied immediately but recorded in the undo log (before-images) for rollback.
   - **Vector writes**: HNSW index inserts are deferred as `DeferredHnswInserts` — the hash field is written, but the vector is not inserted into the HNSW graph until commit.
   - **Graph writes**: Entity modifications are recorded as graph intents.
   - **MQ writes**: `MQ PUBLISH` enqueues are buffered as `MqIntent` entries.

3. **TXN COMMIT** applies deferred vector inserts, flushes MQ intents, writes a `XactCommit` WAL record, and clears the transaction state.

4. **TXN ABORT** replays the undo log in reverse to restore before-images, discards all deferred intents, writes a `XactAbort` WAL record, and clears state.

## Isolation: keys held by an open transaction

A key a transaction writes is **held** until it commits or aborts. `TXN ABORT`
restores every written key's pre-transaction value, so a write another client
made to such a key in between would be overwritten by the abort. That write is
refused instead:

```
A> TXN BEGIN
A> SET k txn
B> SET k other        -> (error) TXNCONFLICT key held by an open transaction
B> FLUSHDB            -> (error) TXNCONFLICT database has keys held by an open transaction
A> TXN ABORT          -> OK   (k is back to its value before the transaction)
B> SET k other        -> OK
```

- Every write path checks: plain commands, `MULTI`/`EXEC` bodies (the queued
  write's element is the error), scripts (`redis.call` raises it), blocking pops
  (a pop that would be served at once is refused; a parked client is not served
  from a held key and is served once the key is released), `MOVE`, `COPY … DB`,
  `MQ`, and routed writes at `--shards > 1`.
- `FLUSHDB`, `FLUSHALL` and `SWAPDB` are refused while an open transaction holds a
  key in a database they would clear or move.
- Reads are not blocked: a client outside a transaction still reads the
  uncommitted value; a reader inside its own transaction does not see it.
- A transaction that writes a key another open transaction holds gets the same
  error, and — like any refused command inside a transaction — can then only be
  aborted (`TXN COMMIT` answers `EXECABORT`).
- A command that answers an error inside a transaction wrote nothing and holds
  nothing.
- Eviction and active expiry skip held keys; an expired held key is reaped
  after the transaction ends. A replica applies its master's stream
  regardless.
- There is no idle timeout: a key stays held until its transaction commits,
  aborts, or its connection closes. `INFO stats` reports `txn_open`,
  `txn_oldest_age_ms`, `txn_held_keys` and `txn_conflicts_refused`.

## Crash recovery

Transaction WAL records (`XactBegin` 0x33, `XactCommit` 0x34, `XactAbort` 0x37) are replayed on startup. Uncommitted transactions (begin without commit/abort) are automatically rolled back during recovery.

## Limitations

- Transactions are **connection-scoped** — a single client connection can have at most one active transaction.
- Cannot nest transactions or mix with `MULTI`/`EXEC`.
- Cross-shard atomicity relies on the WAL commit record — there is no two-phase commit across shards.

## Python SDK

```python
from moondb import MoonClient

client = MoonClient(host="localhost", port=6379)

# Cross-store transactions use raw command execution
client.execute_command("TXN", "BEGIN")
client.set("user:1", "alice")
client.hset("doc:1", mapping={"title": "Hello", "vec": vector_bytes})
client.execute_command("TXN", "COMMIT")
```
