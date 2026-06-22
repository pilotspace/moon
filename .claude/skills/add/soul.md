# Self-improving the voice — how SOUL.md converges to the human

`SOUL.md` is the AI's voice (tone · communication style · trust). It ships as a **proposed starter**
and is **human-owned**; this doc is how it stops being write-once and starts converging to *this*
human. It mirrors `deltas.md` (emit) + `fold.md` (confirm → rewrite), but a confirmed voice delta is
consolidated into **SOUL.md**, not the foundation — because voice is not one of the five competencies.

You (the AI) **emit** voice deltas as `open`. Only the **human** confirms one, and only a confirmed
delta rewrites SOUL.md. **The human's confirm is the only writer** — you never self-approve a voice
rewrite (rewriting someone's voice is theirs to approve, like a foundation consolidation).

## What a voice delta is drawn from

A voice delta is grounded in how the human actually shows up **in session**:
- their **wordings** — the words they reach for, and the words they correct you on (they cross out a
  bit of jargon and write the plain word over it);
- their **flow** — what they skip, what they double-check, where they want the summary before the detail.

NOT from their private **memory** files or anything outside the working session — SOUL learns from what
you observed together, never from reading a personal store.

## The grammar (mirrors deltas.md)

Each voice delta begins on its own tag line; the observation may wrap, and a required `(evidence: …)`
clause closes it:

```
- [VOICE · <status>] <observation about the voice> (evidence: <in-session pointer>)
```

- `<status>` — `open` | `confirmed` | `declined`. A newly emitted delta is **`open`**.
- `<observation>` — what the voice should become ("lead with the decision, not the preamble").
- `(evidence: …)` — **required**, non-empty: a moment in the session (a correction, a re-ask, a
  visible preference). No evidence → it is a guess about the human, not a delta. Drop it.

```
- [VOICE · open] the human strips hedging from my drafts — cut "I think / it seems" and state it plainly
  (evidence: they rewrote two replies this session to remove the qualifier)
```

## The loop — observe → confirm → rewrite

1. **Emit** (OBSERVE) — at a task's observe phase (or on demand), propose 0–N voice deltas as `open`
   from the session's wordings + flow. Surface them in the report; show-before-ask.
2. **Confirm** — the human accepts or declines each. **No SOUL.md write happens without this.**
3. **Rewrite** — on a confirmed delta, edit the routed SOUL.md section, then record the delta line
   (status `confirmed`) at the **top** of the "Voice deltas" ledger (**newest-first**, append-only — a
   declined delta flips to `declined` and stays in place, so "considered, chose not to" is auditable).

## Routing — every voice delta has a SOUL.md home

| the delta is about… | rewrite this SOUL.md section |
|---------------------|------------------------------|
| how I *sound* (warmth, directness, hedging) | **## Tone** |
| how I *structure* what I say (summary-first, show-before-ask, length) | **## Communication style** |
| what keeps the human's *trust* (gates, honesty, what I never do) | **## Trust** |

The rewrite is surgical: refine or append the bullet the delta names; never silently rewrite the rest
of the voice. Every confirmed delta also gets its line in **## Voice deltas** (newest-first).

## Reject codes (the AI is first check, the human the backstop)

<reject_codes>
- `unconfirmed_voice_rewrite` — a SOUL.md write was attempted without a recorded human confirm. The AI
  proposes; it never self-approves. Stop and get the confirm. (The identity-owned floor.)
- `no_open_voice_deltas` — nothing is `open`. The loop is a no-op; do not touch SOUL.md.
- `unroutable_voice_delta` — the observation maps to no SOUL.md section (not tone/style/trust). Fix the
  delta or widen the routing before writing.
</reject_codes>

## Where it plugs in

- **Emit**: `phases/7-observe.md` proposes voice deltas beside its competency/spec deltas.
- **Target**: `SOUL.md` (scaffolded by setup) — its "## Voice deltas" ledger holds the confirmed history.
- **Kin**: `deltas.md` (competency learnings → foundation) and `fold.md` (the same propose→confirm→write
  discipline). This is the voice's version of that discipline; the engine stays judgment-free (no
  `add.py` command writes the voice).
