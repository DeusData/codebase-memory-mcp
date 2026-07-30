# Index Resource Limits

Indexing is intentionally bounded. Before parsing begins, the server validates
the canonical repository root and performs a bounded discovery walk. The worker
and persistence stages continue enforcing the same resolved policy.

If a hard boundary is reached, `index_repository` returns
`resource_limit_exceeded` with the boundary name, observed value, configured
limit, and a remediation hint. A partial index is never published as a complete
one.

## Defaults

| Configuration key | Default | Protects |
|---|---:|---|
| `index_max_files` | `100000` | Accepted source-file count |
| `index_max_directories` | `20000` | Traversed directory count |
| `index_max_entries` | `500000` | Examined filesystem entries |
| `index_max_depth` | `64` | Directory nesting depth |
| `index_max_source_mb` | `4096` | Aggregate accepted source size |
| `index_max_file_mb` | `64` | One source file |
| `index_scan_timeout_seconds` | `30` | Discovery time |
| `index_cpu_cores` | `4` | Indexing worker threads |
| `index_concurrent_jobs` | `1` | Simultaneous physical index workers |
| `index_memory_limit_mb` | `8192` | Worker resident memory, capped by detected budget |
| `index_max_db_mb` | `16384` | Published SQLite database |
| `index_max_staging_mb` | `20480` | Staging database and sidecars |
| `index_max_task_temp_mb` | `24576` | Staging, worker log, and response temporary files |
| `index_cache_max_mb` | `32768` | Total index cache admission |
| `index_min_free_disk_mb` | `4096` | Free disk kept in reserve |
| `index_max_duration_seconds` | `3600` | Worker wall-clock time |
| `index_low_priority` | `true` | Best-effort reduced scheduling priority |
| `index_denied_roots` | empty | Additional exact canonical roots |

Automatic indexing keeps its separate, stricter `auto_index_limit` default of
`50000`.

Set a value with:

```sh
codebase-memory-mcp config set index_max_files 75000
codebase-memory-mcp config set index_memory_limit_mb 4096
```

Separate additional denied roots with semicolons on every platform:

```sh
codebase-memory-mcp config set index_denied_roots '/path/to/aggregate;/path/to/archive'
```

Numeric values must be positive and within the ranges shown by
`codebase-memory-mcp config --help`. `index_denied_roots` may be empty. Invalid
or unknown keys are rejected.

## Root safety

The filesystem root, current user's home directory, and the Codebase Memory
cache directory are always refused as repository roots. This built-in check
cannot be disabled. `index_denied_roots` adds exact canonical roots; it does not
deny all descendants, so individual repositories below an aggregation
directory can still be indexed.

`CBM_ALLOWED_ROOT` remains available for deployments that also need an outer
containment boundary.

`cross-repo-intelligence` is not a source-indexing mode: it reads and updates
already published project databases and does not walk `repo_path`. It therefore
keeps the session containment and cross-repository target-count checks, but does
not apply source-root or discovery limits.

## Failure and recovery

Discovery violations stop before parsing. Worker memory, time, and temporary
output violations terminate the contained process tree. Persistence violations
remove the current operation's staging files and preserve the previously
published database. Reduced scheduling priority is applied where the operating
system permits it; a refusal is logged and does not silently change the other
limits.

The cache limit is an admission check, not an eviction policy. The indexer never
deletes unrelated project indexes automatically.
