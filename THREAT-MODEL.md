# Threat model

codebase-memory-mcp is a local code indexer exposed to coding agents over MCP.
Its optional web surface is an embedded, read-only diagnostic dashboard. It is
not a desktop shell, graph explorer, terminal, project manager, or remote
administration interface.

## Assets and trust boundaries

- Repository contents are untrusted parser and resolver input.
- The local SQLite indexes may contain source paths, symbol names, snippets,
  dependency relationships, commit identifiers, and dirty-worktree metadata.
- MCP clients are trusted to the extent of their enabled toolset. Core MCP may
  index a repository; advanced and admin tools require explicit enablement.
- The dashboard is a separate trust boundary. It can read diagnostics, logs,
  and the allow-listed read-only MCP tools, but it cannot start indexing,
  delete an index, run Cypher, manage ADRs, browse arbitrary directories,
  inspect or kill processes, or open a terminal.
- Update and download traffic occurs only after an explicit CLI command. MCP
  initialization performs no release check or other network request.

## Dashboard controls

- The HTTP listener binds only to `127.0.0.1`; it never binds to an external
  or wildcard address.
- A new 256-bit capability token is generated from the operating-system random
  source for each server process. Protected `/api/*` and `/rpc` requests fail
  without that token.
- The launch URL carries the token in its fragment. The frontend copies it to
  session storage, removes the fragment from the visible URL, and adds it only
  to same-origin diagnostic requests.
- `/rpc` has an explicit read-only method and tool allow-list. Authentication
  does not make mutating or advanced tools available.
- CORS only reflects loopback development origins. Production assets use a
  restrictive Content Security Policy, no external scripts, no analytics,
  `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` for dynamic
  responses.
- Project names are validated before constructing an index path. The
  diagnostic server has no general filesystem-path endpoint.

## Remaining risks

- A hostile repository may exploit a parser defect or cause excessive CPU,
  memory, or disk use. Index untrusted repositories in an OS sandbox and keep
  parser dependencies updated.
- Diagnostic results and logs can reveal private code metadata. Anyone who can
  read the capability from the same user session, browser storage, developer
  tools, screenshots, or process output can access that data until the server
  exits.
- Loopback binding limits network exposure but does not isolate mutually
  untrusted processes running as the same OS user.
- MCP clients with indexing or explicitly enabled admin capabilities can
  change local index state. Client configuration and toolset selection remain
  part of the trusted computing base.
- Availability is not guaranteed against local request flooding or unusually
  expensive read queries. Request-size limits and restricted toolsets reduce,
  but do not eliminate, denial-of-service risk.
