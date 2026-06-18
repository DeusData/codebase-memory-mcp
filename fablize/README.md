# fablize — the discipline layer

This folder is the **procedure layer** of this project. While the C core
(`codebase-memory-mcp`) gives an agent a *map* of the code, fablize gives it a
*method* of working: clarify before building, complete with evidence, investigate
systematically, verify what was rendered, and escalate honestly at the capability ceiling.

It is self-contained and dependency-free (pure-Python stdlib + plain-text packs), so it
works with **any** agent that has a shell — exactly the agents `codebase-memory-mcp`
already configures.

## Contents

| Path | What |
|------|------|
| `AGENTS.md` | the operating block, wired to this project's MCP tools |
| `packs/` | the verified discipline packs (clarify, investigation, verification grounding) |
| `scripts/goals.py` | multi-story loop with an evidence/verification gate + bounded self-correction |
| `scripts/spec.py` | locked-spec store so a clarified spec survives compaction/restart |
| `scripts/metrics.py` | observability over `~/.fablize/events.jsonl` |
| `scripts/bundle.py` | build a portable, tool-agnostic bundle of the disciplines |
| `hooks/destructive_guard.py` | PreToolUse guard that asks before hard-to-reverse commands |
| `tests/` | stdlib unittest suite (no deps) |

## Run the tests

```bash
python3 -m unittest discover -s tests -v
```

## How it composes with the memory layer

See [`../INTEGRATION.md`](../INTEGRATION.md) for how the disciplines call the MCP tools
(`get_architecture`, `search_graph`, `trace_path`, `detect_changes`, …).

MIT licensed.
