# Changelog

## Unreleased — agents-first preview

### Added

- Client-scoped `install --client <name>` with opt-in hooks and instructions.
- Guided `setup [path] --client <name>` for registration, first structural
  index, environment verification, and watcher configuration.
- Default nine-tool MCP surface plus explicit advanced and admin toolsets.
- Deterministic, token-budgeted `get_context` evidence packs.
- Structured MCP results, output schemas, annotations, and session-project
  fallback.
- Worktree fingerprints based on commit, dirty status, and XXH3-128 content
  hashes.
- Candidate-database validation and atomic publication that preserves the last
  successful index on failure.
- Generated product manifest surfaces and reproducible benchmark protocol.

### Changed

- Language support now uses core, preview static-resolver, and experimental
  structural tiers.
- Binary updates preserve indexes, client configurations, and running MCP
  processes.
- The embedded web surface is a local diagnostic dashboard rather than a
  general project-management interface.
- Performance and quality claims require a versioned benchmark artifact.

### Removed

- Electron desktop application, embedded terminal, tray integration, and
  desktop release workflow.
- Lean and the dedicated SystemVerilog parser.
- Runtime `ingest_traces` advertising and its previous false-success response.
- Automatic network update checks during MCP initialization.
