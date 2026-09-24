# WS2-datatype-commands — working notes

Method notes captured while executing PLAN.md (no `add.py` state was mutated).

- **A randomized differential found a data-integrity bug the review did not.** Writing
  moon#1170's `count_while` test against a sorted model, `BPTree::remove` failed on a key just
  inserted. Root cause: `split_internal_and_insert` dropped a separator and the right-most child
  on every non-append insert into a full internal node, and `borrow_from_right_internal` lost a
  subtree count. Every pre-existing test inserted in ascending order — the only order that never
  takes the broken path. Lesson: order-statistic structures need a structural invariant checker
  (`check_invariants`) driven by random, ascending, descending and "rising score, random member"
  patterns WITH removes; a contents-only check (iteration) cannot see an unreachable subtree,
  because the leaf chain still holds it.
- **The oracle corrects the plan.** Redis's `ZRANDMEMBER count>=size` walks `zuiNext`, whose
  iterator starts at the TAIL — descending order. The first implementation (ascending, "score
  order") was wrong and was only caught by the redis-server diff, not by the unit tests written
  from the same (wrong) belief. Diff against the live oracle before claiming parity.
- **Version skew in the oracle.** redis 7.0.15 (the box's oracle) differs from moon's 8.x target
  in: `%.17g` score rendering (WITHSCORES), `%.17Lf` WITHCOORD/GEOPOS, `ZRANK … WITHSCORE`
  (7.2+), set `listpack` encoding (7.2+). The differential normalizes scores to doubles and
  compares coordinates numerically; everything else is byte-exact.
- **Shared target dir aliasing.** Cargo's metadata hash is workspace-relative, so all worktrees
  share fingerprint slots and a changed file older than another worktree's last build is treated
  as fresh. All WS2 dev/test/clippy runs used `--config 'profile.dev.package.moon.debug="limited"'`,
  which gives moon's own units a distinct hash (dependencies stay shared); release builds were
  preceded by `touch` of every file changed since the base and copied in the same command line.
- **Baseline for write-path A/B.** The HEAD baseline's corrupted B+tree does LESS work on random
  inserts/removes (unreachable subtrees make removes fail fast), so a CPU A/B of the new tree
  against it flatters the old code. The honest control was a second release build of the
  bug-fix-only commit (229215e).
