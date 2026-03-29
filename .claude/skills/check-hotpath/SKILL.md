---
name: check-hotpath
description: Scan hot-path code for allocation violations, lock misuse, and performance anti-patterns. Zero-tolerance audit.
---

Hot path invariant checker for moon.

## Hot Path Files

- `src/command/*.rs` — command dispatch
- `src/protocol/*.rs` — RESP parsing
- `src/shard/event_loop.rs` — shard event loop
- `src/shard/spsc_handler.rs` — cross-shard message handling
- `src/io/*.rs` — I/O drivers
- `src/storage/dashtable/*.rs` — hash table lookups

## Checks

### 1. Allocation Violations

Grep for forbidden patterns in hot-path files:
```bash
grep -rn 'Box::new\|Vec::new()\|String::new()\|Arc::new\|\.clone()\|format!\|\.to_string()\|\.to_owned()' src/command/ src/protocol/ src/shard/event_loop.rs src/io/
```

Exceptions: `Vec::with_capacity()` at end of command path is acceptable.

### 2. Lock Violations

```bash
grep -rn 'Mutex::new\|RwLock::new\|\.lock()\|\.read()\.\|\.write()\.' src/command/ src/shard/event_loop.rs src/io/
```

Flag: any lock held across `.await`, any `std::sync` lock (should be `parking_lot`).

### 3. Syscall Violations

```bash
grep -rn 'Instant::now()\|SystemTime::now()\|std::time' src/command/ src/shard/event_loop.rs
```

Should use shard-cached timestamp instead.

### 4. Copy Violations

```bash
grep -rn '\.to_vec()\|\.to_owned()\|copy_from_slice' src/protocol/ src/io/
```

Should use `Bytes::slice()` for zero-copy where possible.

### 5. Branch Density

Find match arms with excessive cases:
```bash
grep -c '^\s*"' src/command/registry.rs 2>/dev/null || echo "Check PHF table"
```

## Output

For each violation:
```
[ALLOC|LOCK|SYSCALL|COPY|BRANCH] file.rs:line
  Pattern: <what was found>
  Severity: HIGH|MEDIUM|LOW
  Fix: <suggested change>
```

Summary: total violations by category.
