# Index resource limits

Index resource limits are optional operator controls for repositories whose
discovery breadth or worker runtime is not known in advance. They are disabled
by default so existing large-repository workloads retain their current behavior.

## Resource profiles

`index_resource_profile` accepts `off`, `balanced`, or `strict` and defaults to
`off`. A profile supplies a baseline; each explicitly stored individual
resource key then replaces its dimension. An explicit `off` disables only that
dimension.

| Dimension | `balanced` | `strict` |
|---|---:|---:|
| Accepted files | 500,000 | 100,000 |
| Traversed directories | 250,000 | 20,000 |
| Directory entries | 2,000,000 | 500,000 |
| Directory depth | 128 | 64 |
| Accepted source | 65,536 MiB | 4,096 MiB |
| Discovery duration | 1,800 seconds | 30 seconds |
| Worker duration | 7,200 seconds | 3,600 seconds |
| Final database | 65,536 MiB | 16,384 MiB |
| Staging artifacts | 81,920 MiB | 20,480 MiB |
| Task temporary artifacts | 98,304 MiB | 24,576 MiB |
| Projected cache | 131,072 MiB | 32,768 MiB |
| Free-disk reserve | 4,096 MiB | 4,096 MiB |

The two profiles answer different questions about worker memory, so their RSS
ceilings are derived rather than tabled.

Balanced protects the host. Its ceiling is 80% of detected host memory, which
leaves an index free to use everything this machine can spare and still stops a
runaway worker before the host is exhausted. A fixed number is deliberately not
used here: large-repository indexing peaks in the tens of gigabytes, so any
round ceiling low enough to feel safe would reject repositories that index
successfully with the profile off. When host memory cannot be read, balanced
falls back to the higher of twice the worker soft budget or 8,192 MiB.

Strict protects the budget. Its ceiling is the lower of the worker soft budget
and 8,192 MiB, which is the point of the profile: a repository that needs more
memory than the daemon budgets for itself fails fast with an attributed
`rss_bytes` violation instead of completing at the host's expense. On a large
repository this is expected to fail, and the failure names the limit and the
observed value so the operator can choose `balanced`, raise
`index_max_rss_mb`, or leave the profile `off`.

Both derived values are held inside the same 64 to 1,048,576 MiB range that
`index_max_rss_mb` accepts, and detected host memory can lower but never raise
an explicitly configured `index_max_rss_mb`.

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

Profile directory counting includes the request root and each non-skipped
directory admitted for traversal. Entry counting happens before ignore,
language, and file-size filtering. Root depth is zero. The monotonic discovery
deadline is checked before opening a directory and before processing each
entry. Equality is allowed for every dimension.

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

## Attempt visibility and freshness

The latest physical attempt for each project is atomically stored in an
owner-private file below `${CBM_CACHE_DIR}/status/`. It records the
`explicit`, `auto`, or `watcher` origin; `queued`, `running`, `completed`,
`failed`, or `cancelled` state; effective profile source; timestamps; and
stable resource failure details when applicable. Daemon startup changes
abandoned `queued` or `running` records to `failed` with
`failure_code=worker_lost`.

After a record exists, `index_status` returns `last_index_attempt` and
`freshness`. A generation is comparable only when matching clean Git snapshots
were observed before and after indexing, and it is `fresh` only while the
current worktree remains clean at that `HEAD`. A different clean `HEAD`, or a
failed watcher rebuild after an observed change, is `stale`. Non-Git roots,
dirty or changed-during-index snapshots, failed Git probes, and corrupt records
are `unknown`. Dirty state is never proof of freshness or staleness. Projects
with no attempt record retain the previous response shape.

A background rebuild that fails reaches only the logs and `index_status`, which
leaves a caller querying an older graph with nothing to warn it. Once a record
is in a `failed` or `cancelled` state, the next answer served from that
project's graph carries a stale-index warning naming the origin, the recorded
finish time, and the limit that ended the attempt. The warning is attached to
`search_graph`, `query_graph`, `trace_path`, `trace_call_path`,
`get_architecture`, `get_code_snippet`, and `search_code`; `index_status` and
`check_index_coverage` already report freshness themselves, and
`index_repository` is the remedy. A JSON payload receives a
`stale_index_warning` field so a structured reader cannot miss it, and a text
or tree payload receives a trailing content block; the answer itself keeps its
position and content. A queued or running rebuild is not yet a failure and
produces no warning, and a completed rebuild clears it.

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
