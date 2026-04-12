# Unsafe Code Policy

Moon enforces a strict gate on `unsafe` blocks. This document complements the
"Unsafe Code" section in [`CLAUDE.md`](CLAUDE.md) with concrete review and
merge requirements.

## Why this matters

`unsafe` is the audit surface where the borrow checker stops protecting us. A
single unsound block can produce data races, use-after-free, or torn-page
corruption that no test will catch until production. We pay a higher review
cost on `unsafe` to keep that risk bounded.

## Hard rules

1. **No new `unsafe` block lands without explicit human approval in the PR.**
   This includes `unsafe impl Send`/`Sync`, `unsafe fn`, and trivial libc
   syscall wrappers. AI assistants and automated refactors must surface every
   new unsafe block to the reviewer.

2. **Every `unsafe` block must have a `// SAFETY:` comment** that names:
   - The exact precondition(s) being upheld.
   - Where the precondition comes from (caller contract, type invariant,
     hardware guarantee, etc.).
   - Why violating it would be UB, in one sentence.

3. **Prefer the safe alternative when the cost is < 100 ns on the hot path.**
   `parking_lot::Mutex` and `RwLock` are cheap enough to replace `UnsafeCell`
   in almost every case. `get_unchecked` should be replaced with
   `debug_assert!` + indexed access unless a benchmark proves otherwise.

4. **Encapsulate `unsafe` behind a safe public API.** A `pub fn` whose body
   contains `unsafe` and whose precondition is "caller must X" is a footgun.
   Make it `unsafe fn` so the caller has to opt in.

5. **Field drop order matters for mmap/FD/raw-pointer types.** When a struct
   holds a resource whose lifetime depends on another field (e.g., `Mmap` +
   `SegmentHandle`), document the field ordering invariant in the struct doc
   comment and add a `// MUST be the last field` comment on the keepalive.

## Review checklist (for PRs touching `unsafe`)

- [ ] Each new `unsafe` block has a `// SAFETY:` comment.
- [ ] Each `unsafe impl Send`/`Sync` is justified by either:
  (a) the type is genuinely thread-safe by construction, or
  (b) a runtime invariant is enforced by the type system (e.g., `!Sync`
      newtype, `thread_local!`, or compile-time feature gate).
  Hand-wavy "we only call this from one thread" is **not** acceptable
  unless the PR description names the specific runtime feature gate that
  enforces it.
- [ ] All raw pointer arithmetic (`ptr.add`, `ptr.offset`) is preceded by a
  `debug_assert!` proving the result is in-bounds, OR the SAFETY comment
  derives the bound from caller-visible preconditions.
- [ ] No `unsafe` is used purely to suppress borrow checker errors. Fix the
  ownership model instead.
- [ ] PR description includes "Unsafe added: N blocks" in the summary, with
  a one-line justification per block.

## Auditing existing unsafe

Run the project's `unsafe-audit` skill / `cargo-geiger` periodically:

```bash
# Count unsafe blocks added on the current branch vs main
git diff main -- 'src/**/*.rs' | grep -cE '^\+.*\bunsafe\b'

# Inventory all unsafe blocks
grep -rn 'unsafe' src/ --include='*.rs' | grep -v '// SAFETY'
```

Any block missing a SAFETY comment is a bug — file an issue.

## Approved patterns

These are pre-vetted and don't require fresh justification, just the
SAFETY comment:

- `libc::close(fd)` in `Drop` for an owned FD.
- `_mm_prefetch` (cannot fault on x86_64).
- `slice::from_raw_parts(self.ptr, self.len)` where `self` owns the
  allocation and `len` is a struct invariant.
- `is_x86_feature_detected!`-gated SIMD intrinsics.
- AArch64 NEON intrinsics from `core::arch::aarch64` under a
  `#[cfg(target_arch = "aarch64")]` gate. NEON is mandatory in ARMv8-A,
  so no runtime detection is needed; the `unsafe` wrapper is a Rust
  formality. The SAFETY comment must still justify pointer validity,
  alignment requirements (note `vld1q_u8` has none, unlike SSE2's
  `_mm_load_si128`), and any shift-by-constant assumptions.
- `MmapOptions::new().map(&file)` over a sealed-after-rename file with a
  refcount-protected directory handle (see
  `vector::persistence::warm_segment::WarmSegmentFiles` for the canonical
  pattern).

## Forbidden without explicit design review

- `transmute` between non-trivially-equivalent types.
- `unsafe impl Send`/`Sync` on types containing `UnsafeCell` or raw
  pointers without a `Mutex`/atomic enforcement.
- `get_unchecked` / `get_unchecked_mut` without a benchmark showing > 5%
  speedup over `[idx]`.
- `mem::uninitialized` / `MaybeUninit::assume_init` without zero-init
  proof.
- Holding `*mut T` across an `await` point.
