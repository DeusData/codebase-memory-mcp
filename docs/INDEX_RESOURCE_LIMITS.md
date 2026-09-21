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
| `index_max_rss_mb` | `off` | `off` or `64..1048576` | Charged memory of the complete worker process tree |
| `index_max_duration_seconds` | `off` | `off` or `1..86400` | Wall-clock duration of the whole index request |

```bash
codebase-memory-mcp config set index_max_rss_mb 8192
codebase-memory-mcp config set index_max_duration_seconds 3600
```

RSS is the charged memory of the contained worker and every descendant, not
the worker's allocation budget and not peak memory. On macOS that quantity is
`phys_footprint` (the same number `cbm_mem_charged()` enforces), not
`resident_size`, which still counts pages the allocator has already handed
back. Linux and Windows use RSS / working set. This hard watchdog is
separate from the internal `CBM_MEM_BUDGET_MB` soft budget. The supervisor
samples the tree at most once every 250 milliseconds so the watchdog does not
turn full process-table enumeration into a busy loop.

Duration is per request: the clock starts at the first worker spawn and is
not reset when crash/hang recovery starts a later attempt. Continuous log
progress does not reset it. It is independent of the existing 15-minute quiet
timeout, which still identifies a worker that stops making progress. A
duration limit shorter than that quiet timeout kills a hung worker before hang
quarantine can name the in-flight file, so the next attempt may hang on the
same file.

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

## Trust and compatibility

Limits are read from the CLI-managed `_config.db`; they are not MCP request
arguments. A supervised parent replaces any caller-supplied internal policy
before spawning its worker, and the worker rejects a missing or incomplete
parent policy.

These settings do not replace or increase `auto_index_limit`, change the 512 MiB
single-file cap, alter workspace-root authorization, or affect
`cross-repo-intelligence`. With all settings `off`, discovery follows the
existing path and the supervisor performs no periodic RSS probe or total-duration
termination.
