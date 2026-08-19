# Index resource limits

Index resource limits are optional operator controls for repositories whose
discovery breadth is not known in advance. They are disabled by default so
existing large-repository workloads retain their current behavior.

## Discovery settings

| Key | Default | Accepted value | Protects |
|---|---:|---:|---|
| `index_max_files` | `off` | `off` or `1..10000000` | Accepted source-file count |
| `index_max_source_mb` | `off` | `off` or `1..1048576` | Accepted source-file bytes |

Set or reset them with the normal configuration command:

```bash
codebase-memory-mcp config set index_max_files 250000
codebase-memory-mcp config set index_max_source_mb 16384
codebase-memory-mcp config reset index_max_files
```

Values use base-10 integers. MiB means 1,048,576 bytes. Empty values, zero,
negative values, suffixes, trailing characters, and values outside the stated
ranges are rejected without changing the stored value.

## Counting and failure semantics

`index_max_files` counts a file only after it passes directory pruning, ignore
rules, filename and suffix filters, language detection, and the existing
per-file size rule. `index_max_source_mb` sums the filesystem sizes of that same
accepted set.

Equality is allowed. The first file or byte that makes an observed value greater
than its limit stops discovery. CBM discards the partial file list and does not
publish a partial graph as a complete index.

For an explicit MCP request the error payload contains:

```json
{
  "status": "error",
  "code": "resource_limit_exceeded",
  "stage": "discovery",
  "resource": "files",
  "observed": 250001,
  "limit": 250000,
  "unit": "files",
  "retryable": true,
  "serving_index_preserved": true,
  "message": "Index discovery exceeded index_max_files"
}
```

The previous database remains available because publication occurs only after a
complete discovery and successful staged build. If no previous database exists,
`serving_index_preserved` is false.

## Trust and compatibility

Limits are read from the CLI-managed `_config.db`; they are not MCP request
arguments. A supervised parent replaces any caller-supplied internal policy
before spawning its worker, and the worker rejects a missing or incomplete
parent policy.

These settings do not replace or increase `auto_index_limit`, change the 512 MiB
single-file cap, alter workspace-root authorization, or affect
`cross-repo-intelligence`. With both settings `off`, discovery follows the
existing unbounded path.
