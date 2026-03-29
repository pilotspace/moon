---
name: add-command
description: Scaffold a new Redis command implementation with dispatch entry, handler, ACL, tests, and consistency test entry. Args: COMMAND_NAME [category].
---

Scaffold a complete new command implementation for moon.

## Usage

- `/add-command LPOS list` — add LPOS command in list category
- `/add-command OBJECT HELP server` — add OBJECT HELP subcommand

## Steps

1. **Read the registry**: Find the PHF dispatch table in `src/command/registry.rs` (or equivalent).

2. **Add handler** in `src/command/{category}.rs`:
   ```rust
   /// COMMAND_NAME key [args...]
   /// Time complexity: O(?)
   /// ACL categories: @{category} @read|@write @fast|@slow
   pub fn cmd_command_name(args: &[Bytes], db: &mut Database) -> Frame {
       if args.len() < MIN_ARGS {
           return Frame::Error(Bytes::from_static(
               b"ERR wrong number of arguments for 'command_name' command",
           ));
       }
       // Implementation
       Frame::Null
   }
   ```

3. **Add dispatch entry** in the PHF table or command registry.

4. **Add ACL category** annotation matching Redis categories.

5. **Add unit test** in the handler file's `#[cfg(test)]` module.

6. **Add consistency test** entry in `scripts/test-consistency.sh`.

7. **Add command test** entry in `scripts/test-commands.sh`.

8. **Verify**: Run `cargo test` and `./scripts/test-consistency.sh --shards 1`.

## Important

- Error messages must match Redis exactly (check with context7).
- Use `Bytes::from_static()` for error strings, not `format!()`.
- Return `Frame::Error` for all error cases, never panic.
