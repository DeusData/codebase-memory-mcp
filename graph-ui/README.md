# CBM Lens Beta

CBM Lens is the local exploration frontend served by `codebase-memory-mcp --ui=true`.
It combines source, the indexed graph and optional browser inference. Source files
are read-only; project architecture decisions can be edited in the ADR workspace.
The frontend was contributed by Bernhard Jackiewicz as CodeAtlasWeb.

## Explore a project

- **Explore** follows the current file or marked source. Its graph requests direct
  dependencies separately from the capped repository layout. File-level impact
  summaries use graph and local Git evidence, with details available on demand.
- **Galaxy** searches nodes, files and folders. Select a scope and expand incoming
  or outgoing relationships by depth and edge type. Overview budgets limit drawing;
  a capped layout does not establish a complete neighborhood. Bounded caches reuse
  positions and verified adjacency, and fresh index generations invalidate evidence.
- **Architecture** presents the repository map, routes, hotspots, system structure
  and behavior. Connectivity clusters are not proof of architectural boundaries.
  Moving arrows show edge direction, not observed execution. Reduced-motion settings
  retain stationary direction markers.
- **Coverage** explains indexing gaps. **System** contains daemon status, indexes,
  diagnostics and MCP configuration. See [configuration and precedence](CONFIGURATION.md).
- **ADR** stores one editable Markdown decision document per project through
  `GET/POST /api/adr`, shared with `manage_adr`. It does not edit repository files.

Use **Add project index** in the project dropdown (or Alt+P) to choose a local
repository, name its index and follow daemon-reported progress. Files are indexed
in place, not uploaded. Closing the dialog does not cancel indexing.

ADR drafts stay in this tab's session storage. Before saving, the UI compares the
server document with the editing baseline and retains conflicting drafts. This is
not an atomic document lock. Saves are limited to 8,000 UTF-8 bytes and a 16 KiB
serialized request. The examples in `fixtures/adr/` are explicitly synthetic and
are never inserted into real projects automatically.

## Local agent

The top-bar agent status opens model configuration; the adjacent chat control folds
or unfolds the conversation. Model downloads require explicit activation. The chat
uses browser-local inference; the selected source is not sent to a remote model.
Model provenance and license limitations are in [THIRD_PARTY.md](THIRD_PARTY.md).

Explorer provides the exact marked text, or the loaded file when nothing is marked.
A caret alone does not narrow the context. One replaceable source block precedes
conversation history; each message captures the current selection. Navigation does
not alter an in-flight request, and Retry uses its original snapshot. Missing or
partial source is disclosed. Token counting uses the selected model's tokenizer;
oversized requests are rejected rather than silently shortened.

Automatic explanations respond to new selections independently of follow-up chat.
Browser history is separated by project, retained for 24 hours, and can be cleared
in Agent configuration.

## Build and preview

From the repository root:

```sh
make -f Makefile.cbm cbm-with-ui
```

This installs the locked frontend dependencies, builds `dist/` and embeds it in the
native binary. For frontend development, run `npm ci` and `npm run dev` in this
directory. Vite listens on 5173 and proxies `/api` and `/rpc` to the local server on
9749. `npm run build` creates a production bundle.

The standalone test preview in `tools/lib/static-proxy.mjs` snapshots `dist/` when
it starts. Restart that preview after building and compare the served `index.html`
asset name with `dist/index.html` before reviewing changes.

The canonical frontend CI entry point is `scripts/ci/test-ui.sh` from the repository
root. It runs unit tests, style and promise checks, acceptance checks and a production
build. Browser smoke commands require a running server and Playwright Chromium;
install that browser explicitly with `npx playwright install chromium` when needed.

## Experimental agent activity

Build with `VITE_EXPERIMENTAL_AGENTS=true` to expose recorded tool activity, hook
setup and graph overlays. The value must be exactly `true`; URL parameters and saved
preferences cannot enable it. Browser-local chat is independent of this flag.
Activity remains off until enabled in the UI. It requires an event adapter and is
not automatic observation of arbitrary coding agents.

The supported adapter is `agents/hooks/atlas-trace.py`. **Connect your agent**
downloads it and offers a project-specific install command. Review and run it locally:

```sh
python3 ~/Downloads/cbm-atlas-trace.py --install-claude --root /absolute/repository --project INDEXED_NAME --daemon-url http://127.0.0.1:9749
```

The installer preserves unrelated settings and hooks, rejects conflicts, symlinks
and malformed configuration, and writes only the selected repository's
`.claude/settings.local.json` and `.claude/hooks/`.
Start a new session in your configured client after installation. Other clients
need their own adapter.

For manual hook setup, set `ATLAS_PROJECT` to the indexed name, optionally
`ATLAS_AGENT_NAME` and `ATLAS_DAEMON_URL`, and pass the tool event on stdin. The hook
records tool, run, sequence, timestamp, path and explicit line ranges. It omits source
contents, tool output and free-form commands, search patterns and descriptions. Paths
can still be sensitive. Destinations must be local; proxies and redirects are refused.

A transactional outbox at `~/.atlas-trace/events.jsonl.outbox.sqlite3` buffers up to
10,000 pending events. `ATLAS_TRACE_FILE` changes the path prefix. Delivery resumes
on the next hook or `python3 agents/hooks/atlas-trace.py --flush`; one flush sends up
to 100 events from one project. The older JSONL/replay tools are fixture utilities.

## Activity and diagnostic APIs

`POST /api/agent-events` accepts `{project, events: [...]}` with at most 100 events.
Each event needs `ts`, `agent`, `run`, integer `seq`, `phase: start|end` and `tool`.
Optional fields include `path`, `lines`, `detail`, `source`, self-reported `intent`
and explicit `replay`. Unknown fields are discarded. The UI distinguishes recorded
facts from self-reports and leaves unmapped paths visible.

The daemon stores activity in `<cache>/activity.db`, independently of replaceable
project indexes. Monotonic cursors, `(project, run, seq)` identities and retired
sequence floors prevent duplicate or expired events from reappearing. Retention is
bounded to 10,000 events; expired history is incomplete. Poll with
`GET /api/agent-events?project=NAME&after=CURSOR&limit=200` on the graph's origin.

Daemon and frontend logs share the SQLite journal. `GET /api/logs` accepts `project`,
`scope=unattributed`, `min_level`, literal `q`, `after` and `lines`; filters apply
before counts and pagination. Unattributed records are not assigned to the selected
project. Responses identify severity, source, receipt time, durable ID and database
generation. A bounded memory fallback is explicitly marked `persistent:false`.

`POST /api/ui-log` batches frontend errors and request failures. Failed journal writes
return 503 for retry. `GET /api/ui-log` reads the same journal; `path` names
`activity.db`, while `export_path` names the rotating compatibility JSONL export.
Changing that export does not change HTTP history. These APIs publish nothing
externally.
