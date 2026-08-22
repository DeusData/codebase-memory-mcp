# Index resource limits

Index resource limits are optional operator controls for repositories whose
discovery breadth or worker runtime is not known in advance. They are disabled
by default so existing large-repository workloads retain their current behavior.

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

## Worker settings

| Key | Default | Accepted value | Protects |
|---|---:|---:|---|
| `index_max_rss_mb` | `off` | `off` or `64..1048576` | Current RSS of the complete worker process tree |
| `index_max_duration_seconds` | `off` | `off` or `1..86400` | Total worker wall-clock duration |

```bash
codebase-memory-mcp config set index_max_rss_mb 8192
codebase-memory-mcp config set index_max_duration_seconds 3600
```

RSS is the current resident memory of the contained worker and every descendant,
not the worker's allocation budget and not peak memory. This hard watchdog is
separate from the internal `CBM_MEM_BUDGET_MB` soft budget. The supervisor
samples RSS at most once every 250 milliseconds so the watchdog does not turn
full process-table enumeration into a busy loop.

Duration uses a monotonic clock from successful spawn. It is independent of the
existing 15-minute quiet timeout: continuous log progress does not reset total
duration, while the quiet timeout continues to identify a worker that stops
making progress.

Equality is allowed. The first RSS or elapsed-duration observation above its
limit starts the existing graceful-to-force process-tree shutdown. CBM reports
terminal only after the tree is quiescent or a bounded containment failure is
explicitly surfaced. Resource termination is not retried and does not
quarantine a source file.

If RSS is enabled and three consecutive probes cannot obtain any trustworthy
tree measurement while the root worker is still running, CBM fails closed with
`code=resource_probe_failed`.

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

Worker limit failures use the same shape with `stage=worker`,
`resource=rss_bytes` and `unit=bytes`, or `resource=duration_ms` and
`unit=milliseconds`. RSS measurement failures use `code=resource_probe_failed`
and omit `observed`, `limit`, and `unit` because no trustworthy observation was
available.

## Storage settings

| Key | Default | Accepted value | Protects |
|---|---:|---:|---|
| `index_cache_max_mb` | `off` | `off` or `1..1048576` | Projected cache bytes after replacement |
| `index_min_free_disk_mb` | `off` | `off` or `1..1048576` | Free bytes reserved on the cache filesystem |

Set or reset these keys through the same `config set` and `config reset`
commands. With both keys `off`, indexing does not scan the cache tree or probe
filesystem capacity.

Projected cache usage is the current cache size, minus the old project
database and SQLite sidecars only when that generation is confirmed valid and
replaceable, plus the current operation's staging artifacts. Other projects
and unrelated files always count toward the limit and are never evicted.

Free space is checked before staging, after the staged build completes, and
immediately before atomic publication. Equality is allowed. An enabled
measurement that cannot be completed fails closed with
`code: "resource_probe_failed"` and
`stage: "storage"`. A limit breach uses `code: "resource_limit_exceeded"`.
Both cases preserve the old serving database. Staging files created by a
terminated supervised worker are tagged with a private task token and removed
after its process tree is quiescent; cleanup cannot match another attempt.

## Trust and compatibility

Limits are read from the CLI-managed `_config.db`; they are not MCP request
arguments. A supervised parent replaces any caller-supplied internal policy
before spawning its worker, and the worker rejects a missing or incomplete
parent policy.

These settings do not replace or increase `auto_index_limit`, change the 512 MiB
single-file cap, alter workspace-root authorization, or affect
`cross-repo-intelligence`. With all settings `off`, discovery follows the
existing path, the supervisor performs no periodic RSS probe or total-duration
termination, and the pipeline performs no cache-tree or free-space probe.
