# Moon Log Schema

Structured tracing fields emitted by Moon's `tracing` instrumentation. All fields use `tracing::instrument` attributes with explicit field names — no unbounded cardinality.

## Sampling

- **Default:** 1/1000 for normal spans, full capture on error
- **Override:** `RUST_LOG=moon=debug` for full tracing (development only)
- **Future:** `--trace-sample-rate` CLI flag (gated behind `otel` feature)

## Span Fields

### Connection Lifecycle

| Span | Fields | Cardinality |
|------|--------|-------------|
| `handle_connection` | `peer_addr`, `client_id`, `shard_id` | bounded (IP + u64 + usize) |
| `handle_connection_sharded_monoio` | `peer_addr`, `client_id` | bounded |

### Replication

| Span | Fields | Cardinality |
|------|--------|-------------|
| `replication_handshake` | `replica_id`, `master_host` | bounded (u64 + hostname) |

### Persistence

| Span | Fields | Cardinality |
|------|--------|-------------|
| `aof_rewrite` | `seq` (manifest sequence) | bounded (u64) |
| `rotate_segment` | (no custom fields — uses function-level span) | N/A |

### Vector Search

| Span | Fields | Cardinality |
|------|--------|-------------|
| `compact_segment` | (no custom fields) | N/A |

## Key Logging Rules

1. **Keys are never logged verbatim.** If a key appears in a log line, it must be hashed (e.g., `xxh64(key)`) to prevent unbounded cardinality and PII exposure.
2. **Command names** are logged via `sanitize_cmd_label()` which maps to a fixed set of ~120 known commands + `"unknown"` catch-all.
3. **Error messages** from client commands are logged at `WARN` level with the command name but not the key or arguments.
4. **Shard IDs** are small integers (0..num_shards), bounded by server config.

## Log Levels

| Level | Usage |
|-------|-------|
| `ERROR` | Unrecoverable I/O failures, persistence corruption, TLS errors |
| `WARN` | Recoverable errors (malformed input, slow subscriber drops, AOF write failures) |
| `INFO` | Server lifecycle (startup, shutdown, config changes, WAL rotation) |
| `DEBUG` | Per-connection lifecycle, SPSC drain, replication state changes |
| `TRACE` | Per-frame parsing, per-command dispatch (extremely verbose) |
