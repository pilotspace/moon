# SETUP REVIEW — moon

production · brownfield · drafted by claude-fable-5 @ 2026-06-11

| # | Decision | Lands in | Tag | Why / Evidence |
|---|----------|----------|-----|----------------|
| 1 | QW4 (per-shard command counters) assumes the Prometheus `counter!` path can split per-shard, and that scrape-read aggregation freshness is acceptable | first-contract | `guessed` | The `metrics` crate's recorder API may not register per-shard handles cleanly (src/admin/metrics_setup.rs uses global macros). Fallback shrinks the win to the bespoke `TOTAL_COMMANDS` atomic only. |
| 2 | QW8 (client registry off the per-batch path) assumes CLIENT LIST tolerates on-change/atomic metadata updates instead of per-batch write-lock updates | first-contract | `guessed` | No test pins CLIENT LIST `cmd`/`age` freshness; redis-tooling expectations inferred. Fallback: per-connection atomics (still lock-free). |
| 3 | v1 milestone scopes review priorities 1·2·5 together and defers FT.SEARCH offload + WAL group commit to v2 | scope | `guessed` | My sizing judgment: one outcome (restore shared-nothing + remove latency floor); priorities 3·4 are different themes. You may prefer FT.SEARCH offload sooner if vector workloads are imminent. |
| 4 | Exit criterion "cross-shard SET/GET p99 < 1ms on monoio" is achievable once the 1ms waker relay is replaced | scope | `guessed` | Inferred from event_loop.rs:1049 comments; no prototype of the eventfd wake exists yet. If wrong: criterion relaxes to "p99 materially below current baseline". |
| 5 | First task = the quick-wins batch (8 items in one task) rather than 8 micro-tasks or starting with the ShardSlice migration | scope | `guessed` | Items are mechanical and share one bench baseline; doing them first de-noises the bigger tasks' measurements. ShardSlice (your priority 1) lands as task 3 with the frozen contract named in MILESTONE.md. |
| 6 | Project goal sentence ("out-scales Redis on multi-core hardware …") | PROJECT.md | `guessed` | Inferred from README positioning + bench tooling; the repo states no explicit mission sentence. |
| 7 | The 8 quick-win code targets exist as described (Mutex'd WAL sender, RwLock'd offsets, global TOTAL_COMMANDS, per-byte backlog loop, Vec::remove(0), unpruned key maps, per-batch registry write, missing TCP_NODELAY) | first-contract | `evidence-grounded` | Each verified by direct read this session: shared_databases.rs:159 · spsc_handler.rs:3089 + state.rs:138 · metrics_setup.rs:401 · backlog.rs:35 · uring_handler.rs:346 · vector/store.rs:192 (zero `.remove` calls) · client_registry.rs:80 + handler_sharded/mod.rs:1868 · grep shows set_nodelay only in moon-bench.rs. |
| 8 | Domain map, invariants, conventions, glossary terms | PROJECT.md / CONVENTIONS.md / GLOSSARY.md | `evidence-grounded` | CLAUDE.md (coding rules, gotchas), README.md, src/ module tree, Cargo.toml. |
| 9 | Dependency allowlist | dependencies.allowlist | `evidence-grounded` | Cargo.toml `[dependencies]` + feature-gated crates, v0.3.0. |
| 10 | Stage = production (full rigor, observe loop) | foundation | `evidence-grounded` | v0.3.0 shipped with signed release assets, CI matrix, fuzz + safety audits (git history, .github/workflows). |
| 11 | Behavior parity is a frozen contract for all v1 tasks (RESP bytes, INFO fields, 132 consistency tests) | scope | `evidence-grounded` | scripts/test-consistency.sh + CI define the parity surface; v1 is perf-only by the user's request. |

Sign: confirm in chat → the agent runs `add.py lock --by "Tin Dang"` (typing it yourself works too)
