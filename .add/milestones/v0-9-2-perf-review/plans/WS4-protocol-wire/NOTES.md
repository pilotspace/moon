# WS4-protocol-wire — working notes

## Design decisions (and the alternatives rejected)
- **Resumable cursor, not Redis-style partial argv.** Moving completed elements out of
  `read_buf` (redis `processMultibulkBuffer`) would leave other readers of the buffer —
  inline GET/SET, the RESP2 subscriber step (tokio parses `read_buf` statelessly), the
  migration/park hand-off, and the query-buffer ceiling (`read_buf.len()`) — looking at a
  buffer that starts mid-frame, and the ceiling would stop bounding the frame. A cursor
  leaves `read_buf` the source of truth.
- **Not pointer-keyed.** `BytesMut` hands the same address back after consume + reserve
  (`reserve` shifts data to the allocation start), so a pointer check passes for different
  bytes. Contract instead: append-only between calls; any foreign consumer resets.
- **Staleness can cost latency, never a frame.** A resumed "complete" is re-verified from
  byte 0 before building; a cursor whose header / last element no longer matches is dropped.
  The remaining exposure — a false "incomplete" after a missed reset — is closed by (a)
  explicit resets at every foreign consumer (monoio inline path, deferral re-encode, tokio
  subscriber step) and (b) construction: cursors only for `*4`+ frames with >= 16 KiB
  validated, and the inline path only consumes `*2`/`*3`. Unit tests re-check every resumed
  "incomplete" against a from-zero scan (`#[cfg(test)]`).
- **No span spill mid-scan.** 16 inline spans; elements past 16 re-walked once after the
  frame is complete. Nothing count-sized is allocated before the frame is present.
- **Direct reads need the shared-buffer rule.** First cut reserved `want` before every read;
  with the allocation still shared (collection elements slice it — moon#1160) that
  allocated a fresh buffer per read: p=1 SADD +107 MB vs +3.8 MB. Fixed with
  `try_reclaim` + "keep filling a shared tail while >= 1 KiB is left" (1693ade).
- **Uninit spare only for plain TCP.** The vendored TLS stream forms `&mut [u8]` over the
  read target; its spare is zeroed first (`IdleParkRead::READ_INTO_UNINIT`).
- **Deferral carry:** frames are kept; bytes only where the next consumer parses bytes
  (RESP2 subscriber loop, migration). Inline fast path skipped while a tail is carried
  (it would answer bytes queued behind the tail first).
- **No counting GlobalAlloc** (would be new `unsafe`): allocation claims are proven
  structurally (pointer adoption / aliasing / capacity) instead.

## Measurement method
- 4-vCPU shared container, load 3–5 from other agents. release-fast binaries:
  `/home/user/wt/bin/baseline-935c555` vs `/home/user/wt/bin/ws4-1693ade` (final; symbols
  checked). Ports 7160–7164. `--shards 1` unless noted, `--appendonly no --save ""
  --maxmemory 0 --disk-offload disable`. redis-server 7.0.15 as reference.
- Throughput is interleaved baseline/ws4 per rep. Because the same binary varies ±25% run
  to run at p=1 on this box, server CPU per request (`utime+stime` delta from /proc over a
  fixed request count) is used as the regression instrument for small commands.
- Scripts (scratchpad): ws4_sweep.py (RPUSH sweep + concurrent PING probe), ws4_ab.sh,
  ws4_ab4.sh, ws4_cpu.sh, ws4_defer_cpu.py, ws4_pin.py / ws4_pin2.py.
