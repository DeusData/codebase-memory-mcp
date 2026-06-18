# fablize — operating disciplines for any AI coding agent

> This is the tool-agnostic version of fablize. `AGENTS.md` is read by Cursor, GitHub
> Copilot, Gemini CLI, Aider, Codex, and other agents the same way `CLAUDE.md` is read by
> Claude Code. Drop this file (and the `packs/` + `scripts/` it references) into a project
> and any agent gains the same completion / verification / investigation discipline.
>
> Principle: a harness cannot raise a model's ceiling. It makes the model reach its *own*
> ceiling by enforcing verification, completion, and investigation as procedure. When the
> ceiling itself is the blocker (open-ended creative detail, self-driven discovery),
> escalate — don't pretend.

Apply only what the task signals — the smallest matching discipline. Overlap only when the
task is genuinely multi-category. With no signal, just follow the baseline.

## The two layers of this project

This project pairs a **memory layer** with this **procedure layer**:

- **Memory (codebase-memory-mcp)** — a structural knowledge graph of the code, exposed as MCP
  tools: `get_architecture`, `search_graph`, `search_code`, `trace_path`, `query_graph`,
  `get_code_snippet`, `detect_changes`, `ingest_traces`, `manage_adr`, `index_repository`, …
  It answers *what the code is* — definitions, callers, data flow, architecture — in
  sub-millisecond queries. Prefer `search_graph` / `search_code` / `trace_path` **instead of
  grep/glob** for finding code, callers, dependencies, and impact.
- **Procedure (fablize, below)** — answers *how to work*: clarify, complete with evidence,
  investigate, verify, escalate.

The disciplines below call the memory tools at the points where they help most (see
`INTEGRATION.md`). When the memory tools are not present, the disciplines still apply — they
degrade gracefully to reading files directly.

## [always] Baseline

- Lead with the outcome. Stay within the requested scope — no incidental refactors.
- Ground every "done" claim in a command you actually ran this session (paste the result).
- Confirm before destructive or hard-to-reverse actions.

## [unfamiliar / multi-file change] Orient first

Before editing code you have not read this session, build the map: follow
`packs/orient-pack.txt` — `get_architecture` for the seams → `search_graph` to locate the
symbols → `trace_path` (inbound) for the blast radius before changing a shared symbol.
Skip for a self-contained edit in a file already in front of you.

- Lead with the outcome. Stay within the requested scope — no incidental refactors.
- Ground every "done" claim in a command you actually ran this session (paste the result).
- Confirm before destructive or hard-to-reverse actions.

## [ambiguous / expensive build] Clarify first

Before building something underspecified (open-ended, multi-file, design/UI, unstated
scope), follow `packs/clarify-pack.txt`: surface the genuine unknowns → ask ONE batched
round of 1–4 targeted questions → lock the agreed spec → then build against it. Persist it:

```bash
python3 scripts/spec.py lock --brief "<summary>" --req "<requirement>" \
  --constraint "<constraint>" --decision "question::answer"
python3 scripts/spec.py show     # run first when resuming an ambiguous build
```

Skip entirely if the request is already specific — asking on a clear task is its own waste.
First resolve what you can from the code (`get_architecture` / `search_graph`) — a question
the graph already answers is not a question for the user.

## [2+ sequential stories] Multi-story loop with a verification gate

Decompose into sequential stories, complete one at a time, produce evidence as you go.
State persists in `./.fablize/` (resume across sessions with `status`).

```bash
python3 scripts/goals.py create --brief "<summary>" \
  --goal "title::verifiable objective" --goal "..."   # the LAST goal must be a verification story
python3 scripts/goals.py next                          # activate the next story + handoff
# ...work that story only...
python3 scripts/goals.py checkpoint --id G001 --status complete --evidence "<concrete evidence>"
# final story is a gate: --verify-cmd "<command>" --verify-evidence "<result>" are required
python3 scripts/goals.py retry --id G001               # reopen a blocked story for another attempt
python3 scripts/goals.py status                        # run first when resuming
```

Rules: `complete` requires non-empty evidence; the final goal cannot complete without a
verify command + its result. A story that is `blocked` twice trips the escalation gate
(see below) — bounded self-correction, never an infinite retry loop.

## [debugging / test failure / unknown cause / review] Investigation protocol

Follow `packs/investigation-protocol.txt`: reproduce first → form 3+ competing hypotheses →
gather evidence per hypothesis → trace the full causal chain (removing the symptom is not
removing the defect) → verify before and after → report the hypotheses you rejected.
The memory tools make this concrete: `trace_path` (mode:"data_flow") *is* the causal chain;
`query_graph` exposes hot-path signals for performance defects; `ingest_traces` folds a
reproduction back into the graph.

## [render / executable artifact: HTML, SVG, game, UI, chart] Verification grounding

Follow `packs/verification-grounding-pack.txt`: run it in the real renderer → observe the
actual output → fix what the observation reveals → re-run. A static parse confirms
well-formed, not correct. For a code change, the analogue is `detect_changes` + `trace_path`
(inbound) to confirm the structural effect and catch a caller you forgot.

## [at the capability ceiling] Escalate

Signals: stuck on the same problem 2+ times (the goals engine trips this automatically),
open-ended creation where detail itself is the value, deep review needing out-of-spec
discovery. These are capability, not procedure. In order: (1) raise the model's thinking
budget / reasoning effort to its maximum; (2) hand off to a stronger model in a fresh
session with an evidence package (symptoms, attempts, failure point, repro); (3) otherwise
report the limit honestly and name where a human must step in.

## Observability

The engines log every event to `~/.fablize/events.jsonl`. Summarize real usage with:

```bash
python3 scripts/metrics.py            # completion rate, escalations, specs locked
```

---

The `scripts/` are pure-Python stdlib (no dependencies) — any agent with a shell can run
them. The `packs/` are plain text — any agent can read them. That is what makes these
disciplines portable across tools.
