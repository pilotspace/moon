---
name: redis-compat
description: Verify moon command behavior matches Redis exactly. Args: command name(s) or --category <name>. Uses redis-cli against both servers.
---

Redis compatibility checker for moon commands.

## Usage

- `/redis-compat GETRANGE SETRANGE` — check specific commands
- `/redis-compat --category string` — check all string commands
- `/redis-compat --all` — check all implemented commands

## Steps

1. Start Redis server and Moon server on different ports:
   ```bash
   redis-server --port 6399 --daemonize yes
   cargo build --release --no-default-features --features runtime-tokio,jemalloc
   ./target/release/moon --port 6400 &
   ```

2. For each command, run identical operations against both servers via `redis-cli`:
   - Normal cases (happy path)
   - Wrong number of arguments
   - Wrong key type
   - Boundary values (empty string, very large, negative)
   - Non-existent keys

3. Compare response bytes character-for-character.

4. Use context7 to fetch latest Redis docs for edge case behavior.

5. Report per command: `[COMPATIBLE|DIVERGENT|UNTESTED]` with specific divergences.

6. Cleanup:
   ```bash
   redis-cli -p 6399 SHUTDOWN NOSAVE
   redis-cli -p 6400 SHUTDOWN NOSAVE
   ```

## Categories

string, list, hash, set, sorted_set, key, stream, connection, pubsub, transaction, scripting, persistence
