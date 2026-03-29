---
name: unsafe-audit
description: Audit all unsafe blocks in moon for soundness, missing SAFETY comments, and potential UB. Run after touching storage, io, or runtime code.
---

Comprehensive unsafe code audit for the moon codebase.

## Steps

1. Find all `unsafe` blocks:
   ```bash
   grep -rn 'unsafe' src/ --include='*.rs'
   ```

2. For each unsafe block, verify:
   - `// SAFETY:` comment is present and accurate
   - No undefined behavior (aliasing, dangling, uninitialized)
   - Type invariants maintained (e.g., valid UTF-8 in HeapString)
   - Pointer arithmetic provably in-bounds
   - Send/Sync impls justified
   - Drop order correct

3. Highlight any NEW unsafe blocks vs main:
   ```bash
   git diff main -- '*.rs' | grep '+.*unsafe'
   ```

4. Output severity-ranked report with [SOUND/UNSOUND/NEEDS_REVIEW] per block.

## Priority files (by unsafe density)

- `src/storage/dashtable/segment.rs` (30 occurrences)
- `src/storage/compact_value.rs` (18)
- `src/io/uring_driver.rs` (9)
- `src/runtime/channel.rs` (8)
- `src/storage/compact_key.rs` (7)
- `src/storage/dashtable/simd.rs` (6)
- `src/shard/uring_handler.rs` (7)
