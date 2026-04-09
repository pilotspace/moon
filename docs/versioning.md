---
title: "Versioning"
description: "Moon's versioning policy and compatibility guarantees"
---

# Versioning Policy

Moon follows [Semantic Versioning 2.0.0](https://semver.org/).

## What SemVer Means for Moon

| Version Bump | When | Compatibility |
|---|---|---|
| **Major** (1.0 → 2.0) | On-disk format change (RDB, WAL, AOF), RESP protocol breaking change, removed commands | Migration required |
| **Minor** (1.0 → 1.1) | New commands, new config options, new features, performance improvements | Wire + disk compatible; upgrade in-place |
| **Patch** (1.0.0 → 1.0.1) | Bug fixes only | Wire + disk compatible; drop-in replacement |

## Format Versioning

Moon writes a format version into persistence files:

| File | Version Field | Current |
|---|---|---|
| RDB snapshot | Magic header: `MOON` + version byte | v1 |
| WAL v3 segments | Segment header version field | v3 |
| AOF manifest | Manifest version field | v1 |

**Forward compatibility:** Moon refuses to load files with a version higher than it understands, with a clear error message:
```
Error: RDB version 2 is not supported by this Moon build (max: 1). Upgrade Moon first.
```

**Backward compatibility:** Moon loads files from the same major version. Minor version differences within the same major are handled by additive field defaults.

## Pre-1.0 Stability

During 0.x development:
- On-disk formats may change between minor versions
- Wire protocol is stable (RESP2/RESP3)
- Config options may be added/renamed (not removed without deprecation)
- Command behavior matches Redis semantics where documented

## Upgrade Process

1. Stop the replica first, then the master (if replicated)
2. Replace the `moon` binary
3. Start master, then replica
4. Verify with `INFO server` (check `moon_version`)

## Downgrade Process

Downgrade is supported **within the same minor version** (e.g., 1.0.2 → 1.0.1).
Cross-minor downgrade is **not guaranteed** — new persistence features may write formats the old version cannot read.

---

*See [Production Contract](PRODUCTION-CONTRACT.md) for SLO guarantees per version.*
