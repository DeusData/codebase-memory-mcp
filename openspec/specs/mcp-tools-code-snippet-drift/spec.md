# mcp-tools-code-snippet-drift Specification

## Purpose
Defines how the MCP source-reading tools behave when a file on disk no longer matches the metadata recorded at index time: they must detect the drift and report it explicitly instead of silently serving source text sliced with stale line coordinates.

## Requirements

### Requirement: get_code_snippet detects index-to-disk drift before serving source
When a resolved node's file has changed on disk since indexing (freshness `metadata_changed`) or is missing on disk, `get_code_snippet` SHALL NOT serve source text read from the live file using the node's indexed `[start_line, end_line]` coordinates. The response SHALL instead mark the result as drifted with a `source_drift` boolean, a `freshness` field naming the state (`metadata_changed` or `missing`), and a `source` value that states no source is available because the file changed after indexing. When the file matches the index (`metadata_match`) the tool SHALL return the full source slice exactly as before.

#### Scenario: snippet requested for a file edited after indexing
- **WHEN** a file was indexed, then its content is modified on disk (mtime or size changes) without re-indexing, and `get_code_snippet` resolves a symbol in that file
- **THEN** the response contains `source_drift: true`, `freshness: "metadata_changed"` or `"missing"`, and no source text sliced from the live file with the stale indexed coordinates

#### Scenario: snippet requested for a file whose metadata matches the index
- **WHEN** `get_code_snippet` resolves a symbol in a file whose recorded mtime/size equal the file on disk
- **THEN** the response is unchanged from today: `source` carries the live slice of the file's current content at the indexed coordinates

### Requirement: search_code does not attach drifted source text to results
`search_code` SHALL NOT attach `source` or `context` text read from a file whose on-disk metadata differs from the index (`metadata_changed` or `missing`) using the stale indexed `[start_line, end_line]` ranges. Such result items SHALL carry a `source_drift` marker and a `freshness` field naming the state. Items for files whose metadata matches the index SHALL keep today's `source`/`context` attachment.

#### Scenario: full-mode search hit in a file edited after indexing
- **WHEN** `search_code` (mode `full`) finds a match in a file whose metadata no longer matches the index
- **THEN** the result item carries `source_drift: true` and `freshness: "metadata_changed"` (or `"missing"`) and no `source` text sliced from the live file with stale coordinates

#### Scenario: full-mode search hit in a file matching the index
- **WHEN** `search_code` (mode `full`) finds a match in a file whose recorded metadata equals the file on disk
- **THEN** the result item carries the `source` window around the match exactly as today

#### Scenario: context-mode search hit in a drifted file
- **WHEN** `search_code` is called with `context` lines against a file whose metadata no longer matches the index
- **THEN** the result item carries `source_drift: true` and `freshness: "metadata_changed"`, and does not attach `context` text for a symbol attribution that cannot be trusted
