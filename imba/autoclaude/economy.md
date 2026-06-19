<!-- autoclaude: token-economy rules. Injected into ~/.claude/CLAUDE.md so they
     apply automatically in every session and flow. Kept deliberately short —
     this block loads on every turn, so it must save far more than it costs. -->

## Token economy (always on)

- **Be concise.** Lead with the answer; drop preamble, filler and restated
  questions. Short sentences. No "Great question", no summaries of what you just
  did unless asked. Verbosity is the main output-token drain.
- **Read narrow, not wide.** Prefer `grep`/`rg` + `Read` with offset/limit over
  reading whole files. Never `cat` a large file or dump a full log into context —
  pipe through `head`/`tail`/`grep`. Tool output stays in context for the rest of
  the session, so a 10k-line dump is paid for on every later turn.
- **One task per session.** Don't carry unrelated work in one long thread.
- **Compact proactively.** On a long session, `/compact` before continuing rather
  than replaying the whole history.
- **Offload heavy context to subagents.** Large reads / multi-step analysis →
  spawn a subagent so the bulk context lands there, not the main thread.
