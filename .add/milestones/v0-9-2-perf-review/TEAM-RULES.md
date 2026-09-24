# TEAM RULES — how every workstream agent builds, tests, commits and reports

This box: 4 vCPU x86_64 Linux, 15 GB RAM, ~25 GB free disk, shared by up to 7 agents.
Building the 414K-line crate is the scarce resource. These rules exist so the team
does not OOM, fill the disk, or corrupt each other's work.

## 1. Where you work
- Your git worktree: `/home/user/wt/<WS-ID>` on branch `perf/<ws-id>`. Work ONLY there.
  Never `cd` into `/home/user/moon` or another worktree to edit, build, or commit.
- Never push, never merge, never rebase onto other branches, never force anything.
  The orchestrator integrates your branch.

## 2. Building (shared target dir — builds serialize on cargo's lock; that is intended)
- ALWAYS: `export CARGO_TARGET_DIR=/home/user/wt/target CARGO_INCREMENTAL=0` (dependencies are pre-built there;
  incremental caches are OFF — wave 1 grew them 7 GB in 40 min across worktrees).
- ARTIFACT ALIASING: cargo's metadata hash is workspace-relative, so every worktree's moon artifacts have the
  SAME file names in the shared target and another agent's build can overwrite them at any moment. Run tests
  ONLY via `cargo test …` (build+run in one invocation) — never execute a test binary by path afterwards; for
  release builds use `cargo build --profile release-fast --bin moon && cp <target>/release-fast/moon /home/user/wt/bin/<ws>-<label>`
  in ONE command line and measure only the copied binary.
- Iterate with `cargo check --lib` (fast). Unit tests: `cargo test --lib <filter>`.
  Integration tests: ONLY by name, `cargo test --test <name> [filter]` — NEVER a bare
  `cargo test` (272 test binaries ≈ 10 GB + an hour of CPU).
- Before your LAST commit: `cargo fmt`, `cargo check --lib --no-default-features --features runtime-tokio,jemalloc`,
  `cargo clippy --lib -- -D warnings`, and the same clippy with the tokio feature set.
- If you touched `#[cfg(test)]` code or a bench, also `cargo check --lib --tests` (unit tests compile).
- Release measurement builds: `cargo build --profile release-fast --bin moon` — at most TWO per
  workstream (≈4+ min each under contention). Copy the resulting binary to
  `/home/user/wt/bin/<ws-id>-<label>` immediately (the shared target path is overwritten by
  the next agent's build). The HEAD baseline binary is `/home/user/wt/bin/baseline-935c555`.
- Disk guard: before any build run `df -h /`; if Avail < 4G, STOP building, do not delete
  anything outside your worktree, and report it in SUMMARY.md.
- Never `cargo clean`, never delete `/home/user/wt/target`, never `rm -rf` outside your worktree.
- If a build is "Blocking waiting for file lock", that is another agent building — wait; it is normal.

## 3. Running servers for measurement
- Use ports in YOUR range only (so agents never collide): WS1 7100–7119, WS2 7120–7139,
  WS3 7140–7159, WS4 7160–7179, WS5a 7180–7199, WS5b 7200–7219, WS6 7220–7239,
  WS7 7240–7259, WS8 7260–7279, WS9 7280–7299, WS10 7300–7319.
- `redis-server` 7.0.15 and `redis-benchmark` are on PATH (redis as the oracle / baseline).
- Data dirs under `/home/user/wt/<WS-ID>/.bench/` (gitignored? if not, never commit them).
- Always `--appendonly no --save "" --maxmemory 0 --disk-offload disable` unless the finding
  needs persistence. Shut every server you start down before you finish (`redis-cli -p P shutdown nosave`).
- Numbers from this box are RELATIVE evidence only (4 vCPU container, same-host client). Report
  interleaved A/B (baseline binary vs yours, ≥3 alternating reps) or complexity ratios, never a
  single absolute run. Complexity fixes: prove O() changed with a size sweep (e.g. N=10K vs 1M).

## 4. Tests you must add
- Every fix ships a regression test that FAILS on HEAD `935c555` and passes after (red → green):
  correctness asserts, wall-time/op-count bounds, allocation or RSS bounds as appropriate.
- New parsers/decoders → fuzz target + entry in BOTH matrices of `.github/workflows/fuzz.yml`.
- New atomic state machine → a loom model (see `tests/loom_response_slot.rs`).
- Keep replies byte-identical to redis; where redis-cli comparison is cheap, do it
  (`redis-compat` skill describes the method).

## 5. Commits
- One commit per issue (or more if an issue splits naturally) — NEVER bundle two issues in one commit.
- Message: `perf(<area>): <imperative summary> (moon#NNNN)` / `fix(<area>): … (moon#NNNN)`,
  body = mechanism, evidence (numbers or test names), and anything deferred.
  End every commit message with the two trailer lines given in your task prompt.
- Do NOT edit shared orchestrator artifacts: `CHANGELOG.md`, `.add/state.json`,
  `.add/milestones/v0-9-2-perf-review/MILESTONE.md`, `TEAM-RULES.md`, `.add/PROJECT.md`,
  `.add/CONVENTIONS.md`, `CLAUDE.md`, `README.md`, or any other workstream's plan dir.
- Stay inside your OWNED FILES (see your PLAN.md). If a fix truly needs a file you do not own,
  make the smallest possible edit, isolate it in its own commit, and list it under
  "Cross-ownership edits" in SUMMARY.md so the orchestrator can resolve merges.

## 6. SUMMARY.md (your final deliverable, committed last in your plan dir)
```
# <WS-ID> SUMMARY
## Per-issue verdict
| issue | verdict (FIXED/PARTIAL/DEFERRED) | commits | evidence (test names, numbers) | follow-ups |
## Measurements (method, reps, raw numbers)
## Cross-ownership edits
## Risks / things the orchestrator must re-check at integration
## Self-evaluation (0–1): Completeness · Clarity · Practicality · Optimization · Edge cases · Self-evaluation
```
If any self-score < 0.9, refine the work (or say precisely why it cannot reach 0.9 here) before finishing.
If the harness refuses a subagent Write of SUMMARY.md, do not work around it: put the full SUMMARY.md content in your
final report and the orchestrator commits it.
