#!/usr/bin/env bash
# Raw-RESP compatibility diff: Moon vs a real redis-server, RESP2 + RESP3.
#
# Unlike scripts/test-commands.sh, this does NOT go through redis-cli. redis-cli
# renders replies to text, which destroys the reply type before any comparison
# can see it — the reason ~22 type-level defects survived into v0.8.5. This
# harness speaks RESP on a raw socket and compares TYPE, then SHAPE, then VALUE.
#
# Usage:
#   scripts/test-client-compat.sh                       # full matrix
#   scripts/test-client-compat.sh --strict              # + fail on stale waivers
#   scripts/test-client-compat.sh --contexts standalone,multi,pipeline
#   scripts/test-client-compat.sh --protocols resp3
#   scripts/test-client-compat.sh --info-manifest
#   scripts/test-client-compat.sh --filter zrandmember
#
# Env:
#   MOON_BIN    moon binary under test (default: this checkout's release build)
#   REDIS_BIN   redis-server           (default: from PATH)
#
# Exit: 0 all passed or waived · 1 unwaived difference · 2 harness could not run
#       (ERR_NO_ORACLE | ERR_NO_MOON | ERR_SERVER_TIMEOUT | ERR_BAD_MANIFEST |
#        ERR_UNREASONED_WAIVER | ERR_STALE_WAIVER)
#
# A missing redis-server is a FAILURE, never a skip: a differential harness with
# no oracle proves nothing, and a green skip would be a lie.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$ROOT/scripts/client-compat/differ.py" "$@"
