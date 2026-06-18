# How the two layers compose

This project is one product made of two complementary layers:

| Layer | Folder | Answers | Form |
|-------|--------|---------|------|
| **Memory** | `src/`, `internal/`, … (the C core) | *What is the code?* — definitions, callers, data flow, architecture | MCP server, 14 tools, SQLite graph |
| **Procedure** | `fablize/` | *How do I work on it?* — clarify, complete, investigate, verify, escalate | stdlib Python + plain-text packs |

The memory layer gives the agent a **map**; the procedure layer gives it a **method**. Neither
replaces the other — a map without a method wanders, a method without a map crawls file by file.

## Where the procedure calls the memory

The fablize disciplines invoke the MCP tools at the exact points they help most:

| Discipline (`fablize/packs/…`) | Calls these memory tools | Why |
|---|---|---|
| **orient-pack** | `index_repository`, `get_architecture`, `search_graph`, `get_code_snippet`, `trace_path` | Build the map before editing — know the seams and the blast radius. |
| **clarify-pack** (step 0) | `get_architecture`, `search_graph`, `search_code` | Answer unknowns from the code before asking the user — cheaper than a question. |
| **investigation-protocol** (steps 3–4) | `search_graph`, `trace_path` (data_flow), `get_code_snippet`, `query_graph`, `ingest_traces` | `trace_path` *is* the causal chain; `query_graph` exposes hot-path signals. |
| **verification-grounding** | `detect_changes`, `trace_path` (inbound) | Confirm the structural effect of a change and catch a forgotten caller. |
| **spec-lock decisions** (`spec.py`) | `manage_adr` (optional) | A locked architectural decision can be recorded as an ADR in the graph. |

All of this is **prompt-level wiring** — plain text and tool calls. No C was modified; the C
core stays byte-for-byte upstream, so `git pull upstream` merges cleanly. The procedure layer
also degrades gracefully: if the memory tools are absent, every discipline still applies by
reading files directly.

## Design boundary (deliberate)

fablize is **not** reimplemented as MCP tools inside the C server. Its engines stay as
dependency-free Python the agent drives from a shell — the same shell every agent that
codebase-memory-mcp configures already has. This keeps the procedure layer portable, testable
in isolation (`fablize/tests/`), and independent of the C build.

See `fablize/AGENTS.md` for the operating block and `fablize/README.md` for the layer's contents.
