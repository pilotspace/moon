#!/usr/bin/env python3
"""Seed test data for Phase 101 benchmarks via redis-cli --pipe."""
import subprocess
import sys

def resp(*args):
    """Build RESP protocol for a command."""
    parts = [f"*{len(args)}\r\n"]
    for a in args:
        s = str(a)
        parts.append(f"${len(s)}\r\n{s}\r\n")
    return "".join(parts)

def pipe(port, commands):
    """Send commands via redis-cli --pipe."""
    data = "".join(commands)
    p = subprocess.run(
        ["redis-cli", "-p", str(port), "--pipe"],
        input=data.encode(), capture_output=True
    )

def seed(port):
    cmds = []

    # Lists
    cmds.append(resp("DEL", "mylist", "blist", "blsrc", "bldst"))
    for i in range(1, 10001):
        cmds.append(resp("RPUSH", "mylist", str(i)))
    for i in range(1, 50001):
        cmds.append(resp("RPUSH", "blist", str(i)))
    for i in range(1, 50001):
        cmds.append(resp("RPUSH", "blsrc", str(i)))
    pipe(port, cmds)

    # Hash
    cmds = [resp("DEL", "myhash")]
    for i in range(1, 101):
        cmds.append(resp("HSET", "myhash", f"field{i}", f"value{i}"))
    pipe(port, cmds)

    # Sets
    cmds = [resp("DEL", "myset1", "myset2", "myset3", "smvsrc", "smvdst")]
    for i in range(1, 201):
        cmds.append(resp("SADD", "myset1", f"m{i}"))
    for i in range(50, 251):
        cmds.append(resp("SADD", "myset2", f"m{i}"))
    for i in range(100, 301):
        cmds.append(resp("SADD", "myset3", f"m{i}"))
    for i in range(1, 20001):
        cmds.append(resp("SADD", "smvsrc", f"m{i}"))
    pipe(port, cmds)

    # Sorted sets
    cmds = [resp("DEL", "myzset1", "myzset2", "myzset3", "bzset", "zdst")]
    for i in range(1, 201):
        cmds.append(resp("ZADD", "myzset1", str(i), f"m{i}"))
    for i in range(50, 251):
        cmds.append(resp("ZADD", "myzset2", str(i), f"m{i}"))
    for i in range(100, 301):
        cmds.append(resp("ZADD", "myzset3", str(i), f"m{i}"))
    for i in range(1, 50001):
        cmds.append(resp("ZADD", "bzset", str(i), f"m{i}"))
    pipe(port, cmds)

    # HyperLogLog
    cmds = [resp("DEL", "hll1", "hll2", "hll3")]
    for i in range(1, 1001):
        cmds.append(resp("PFADD", "hll1", f"e{i}"))
    for i in range(500, 1501):
        cmds.append(resp("PFADD", "hll2", f"e{i}"))
    pipe(port, cmds)

    # Function library
    body = '#!lua name=benchlib\nredis.register_function("echo1", function(keys, args) return args[1] end)'
    subprocess.run(
        ["redis-cli", "-p", str(port), "FUNCTION", "FLUSH"],
        capture_output=True
    )
    subprocess.run(
        ["redis-cli", "-p", str(port), "FUNCTION", "LOAD", "REPLACE", body],
        capture_output=True
    )

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6379
    seed(port)
