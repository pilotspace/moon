#!/usr/bin/env python3
"""Fix broken cross-references surfaced by `mkdocs build --strict`.

These links pre-date the MkDocs migration and were never validated (the docs
were orphaned from the Mintlify nav). Three kinds of fix:

  - link target is now a site page      -> repoint to the .md page
  - target is a tracked repo file        -> absolute GitHub blob URL (works on
    GitHub and on the site)
  - target is dead (TODO/SUPPORT/.planning, untracked or nonexistent)
                                          -> de-link (keep the text, drop the href)

Literal, reviewable replacements. Re-runnable: no-ops once applied.
"""
from __future__ import annotations

from pathlib import Path

GH = "https://github.com/pilotspace/moon/blob/main"

# file -> list of (old, new) literal replacements
FIXES: dict[str, list[tuple[str, str]]] = {
    "CONTRIBUTING.md": [
        ("[CLAUDE.md](CLAUDE.md)", f"[CLAUDE.md]({GH}/CLAUDE.md)"),
        ("[README.md](README.md)", f"[README.md]({GH}/README.md)"),
        (
            "[CLAUDE.md § Allocations on Hot Paths](CLAUDE.md#allocations-on-hot-paths)",
            f"[CLAUDE.md § Allocations on Hot Paths]({GH}/CLAUDE.md#allocations-on-hot-paths)",
        ),
    ],
    "UNSAFE_POLICY.md": [
        ("[`CLAUDE.md`](CLAUDE.md)", f"[`CLAUDE.md`]({GH}/CLAUDE.md)"),
    ],
    "docs/OPERATOR-GUIDE.md": [
        ("[BENCHMARK.md](../BENCHMARK.md)", "[BENCHMARK.md](benchmark-report.md)"),
        ("[Phase 190 plans](../.planning/phases/190-memory-observability/)", "Phase 190 plans"),
        ("[Phase 191 plans](../.planning/phases/191-allocator-ux/)", "Phase 191 plans"),
    ],
    "docs/STORAGE-FORMAT-V1.md": [
        ("[`docs/SUPPORT.md`](SUPPORT.md)", "`docs/SUPPORT.md`"),
        ("[`SECURITY.md`](../SECURITY.md)", "[`SECURITY.md`](security.md)"),
        (
            "[`.planning/milestones/v0.3.0-ROADMAP.md`](../.planning/milestones/v0.3.0-ROADMAP.md)",
            "`.planning/milestones/v0.3.0-ROADMAP.md`",
        ),
    ],
    "docs/PRODUCTION-CONTRACT.md": [
        ("[SECURITY.md](../SECURITY.md)", "[SECURITY.md](security.md)"),
        (
            "[`.planning/MOON-DATAFLOW-WALKTHROUGH.md`](../.planning/MOON-DATAFLOW-WALKTHROUGH.md)",
            "`.planning/MOON-DATAFLOW-WALKTHROUGH.md`",
        ),
        ("[`.planning/REQUIREMENTS.md`](../.planning/REQUIREMENTS.md)", "`.planning/REQUIREMENTS.md`"),
    ],
    "docs/operations/reclamation-runbook.md": [
        ("[TODO.md](../../TODO.md)", "TODO.md"),
        ("[CLAUDE.md](../../CLAUDE.md)", f"[CLAUDE.md]({GH}/CLAUDE.md)"),
    ],
    "docs/operations/reclamation-slo.md": [
        ("[TODO.md](../../TODO.md)", "TODO.md"),
    ],
    "docs/guides/getting-started.md": [
        ("[Persistence guide](../persistence.mdx)", "[Persistence guide](persistence.md)"),
        ("[TLS setup](../tls.mdx)", "[TLS setup](tls.md)"),
    ],
    "docs/guides/migration.md": [
        ("[redis-compat.md](/docs/redis-compat.md)", "[redis-compat.md](../redis-compat.md)"),
    ],
}


def main() -> int:
    total = 0
    for rel, pairs in FIXES.items():
        path = Path(rel)
        if not path.exists():
            print(f"skip (missing): {rel}")
            continue
        text = path.read_text(encoding="utf-8")
        n = 0
        for old, new in pairs:
            count = text.count(old)
            if count:
                text = text.replace(old, new)
                n += count
        if n:
            path.write_text(text, encoding="utf-8")
        print(f"  {rel}: {n} replacement(s)")
        total += n
    print(f"\nTotal: {total} replacement(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
