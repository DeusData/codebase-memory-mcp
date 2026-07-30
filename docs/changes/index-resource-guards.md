# Index Resource Guard Design

## Problem

An explicit `index_repository` request currently trusts the requested directory
and lets discovery, parsing, and persistence grow until the operation finishes
or the host runs out of resources. This is unsafe for aggregation directories,
accidental filesystem roots, and repositories with unexpectedly large generated
trees.

## Behavior contract

The indexer resolves one immutable resource policy at admission time. The same
policy is passed to discovery, the worker process, and persistence. A limit
failure is reported as `resource_limit_exceeded`; it is never reported as a
successful or merely empty index.

### BDD scenarios

1. **Dangerous root**
   - Given an explicit request whose canonical path is the filesystem root,
     current user's home directory, or the cache directory
   - When `index_repository` validates the request
   - Then it refuses the request before discovery or a worker is started
   - And the response identifies the `repository_root` policy

2. **Bounded discovery**
   - Given a tree that exceeds the configured file, directory, entry, depth, or
     aggregate indexable-source-byte limit
   - When discovery reaches that boundary
   - Then discovery stops immediately and identifies the exact boundary
   - And no partial file list is passed to parsing

3. **Bounded compute**
   - Given default configuration on a large host
   - When an index job starts
   - Then at most four indexing workers are used
   - And only one physical indexing job runs at a time
   - And the worker runs at reduced scheduling priority where supported

4. **Bounded memory and time**
   - Given a supervised worker that stays above its hard memory limit or runs
     longer than its duration limit
   - When the supervisor polls it
   - Then the process tree is terminated
   - And the operation returns `resource_limit_exceeded`, including the limit
     name and configured value

5. **Bounded persistence**
   - Given an index whose staging database, final database, temporary task
     footprint, or cache footprint would exceed policy
   - When persistence checks the next publish step
   - Then the new index is not published
   - And the previously published database remains usable
   - And abandoned staging and sidecar files for this operation are removed

6. **Configuration validation**
   - Given an unknown resource key, a non-integer value, zero, or a value outside
     the supported range
   - When `config set` is invoked
   - Then configuration is rejected without changing the previous value

7. **Public-repository hygiene**
   - Given the final change
   - When tracked content is scanned
   - Then it contains only generic examples such as `/path/to/repo`
   - And it contains no developer usernames, workstation paths, private
     repository names, tokens, or index contents

8. **Cross-repository database linking**
   - Given `mode="cross-repo-intelligence"` and existing source and target
     databases
   - When `index_repository` handles the request
   - Then it preserves session containment and target validation
   - And it does not apply source-root or discovery limits because it does not
     walk the source directory

## Resource policy

All byte-size settings use MiB at the user-facing configuration boundary and
are converted with overflow checks.

| Key | Default | Decision |
|---|---:|---|
| `index_max_files` | 100,000 | Accepted source files |
| `auto_index_limit` | 50,000 | Stricter automatic-admission file limit |
| `index_max_directories` | 20,000 | Traversed directories, including root |
| `index_max_entries` | 500,000 | Filesystem entries examined |
| `index_max_depth` | 64 | Root is depth 0 |
| `index_max_source_mb` | 4,096 | Aggregate accepted source bytes |
| `index_max_file_mb` | 64 | Oversized files are skipped and reported |
| `index_scan_timeout_seconds` | 30 | Discovery deadline |
| `index_cpu_cores` | 4 | Maximum indexing worker threads |
| `index_concurrent_jobs` | 1 | Maximum physical index workers |
| `index_memory_limit_mb` | 8,192 | Also capped by detected available memory |
| `index_max_db_mb` | 16,384 | Published database ceiling |
| `index_max_staging_mb` | 20,480 | Staging DB plus SQLite sidecars |
| `index_max_task_temp_mb` | 24,576 | Staging plus worker log/response footprint |
| `index_cache_max_mb` | 32,768 | Admission ceiling; no automatic eviction |
| `index_min_free_disk_mb` | 4,096 | Reserved free space |
| `index_max_duration_seconds` | 3,600 | Wall-clock worker deadline |
| `index_low_priority` | `true` | Best-effort lower scheduling priority |
| `index_denied_roots` | empty | Additional canonical roots, semicolon-separated |

Built-in dangerous roots cannot be disabled. Operators may lower or raise
numeric values within documented validation ranges. `0` never means unlimited;
it is rejected.

## Data model and state transitions

`cbm_index_limits_t` is the resolved, byte-based policy. Discovery additionally
returns counters and a stable violation enum. The index request progresses:

`admitted -> preflight discovery -> worker -> staging persistence -> publish`

Any resource violation transitions to `resource_limit_exceeded`. Before publish,
failure removes only files owned by the current staging operation. After a
successful atomic rename, the old database is already replaced and later
best-effort artifact export cannot claim rollback.

## Shared entry paths

- MCP and one-shot CLI both enter through `handle_index_repository`.
- Daemon admission resolves concurrency and memory caps once.
- Supervised workers receive only resolved internal numeric arguments, not
  caller-controlled overrides.
- Automatic indexing uses the same policy with the stricter
  `auto_index_limit`.
- `cross-repo-intelligence` remains a database-linking path with its existing
  target-count and mutation guards; it does not enter source discovery.

## Tests and fixtures

- Discovery fixtures use temporary generic directory trees.
- MCP integration tests exercise dangerous-root rejection and structured error
  output without spawning a worker.
- Pipeline tests create a valid old database, force a small staging/DB limit,
  and verify the old database remains.
- Supervisor tests use deterministic process/resource probes rather than large
  real allocations, including worker-owned temporary output.
- A real supervised-worker integration test proves the parent's resolved policy
  reaches the child and replaces caller-supplied internal policy data.
- Configuration tests verify valid overrides and fail-closed parsing.
- Existing cross-repository integration tests verify target validation,
  cancellation, deduplication, missing databases, and source-name overrides.

## Installation and migration

No database migration is required. Existing indexes remain readable. The new
limits affect the next indexing operation. Existing environment variables remain
compatible, but the resolved policy always chooses the safer lower effective
limit.

## Non-goals

- Exact percentage CPU throttling. Worker-count tokens, single-job admission,
  and lower process priority provide portable bounded CPU use.
- Automatic deletion of unrelated cache entries. Cache excess blocks new work
  and reports remediation; it does not delete another project's data.
- Following symlinks, junctions, or paths outside the canonical requested root.
- Per-request public overrides that let an untrusted MCP caller weaken operator
  policy.

## Directly applicable repository rules

- "All code must be properly formatted before committing."
- "Maintain appropriate tests for your changes following the test hierarchy."
- "Prefer explicit error handling."
- "Do NOT create directories or files directly in the repository root."
- Behavior changes follow red, green, refactor: tests precede production code.
- Tracked content must be suitable for a public open-source repository.
