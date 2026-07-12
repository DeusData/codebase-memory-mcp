# Global Memory Architecture

> Looking for setup and task-oriented examples? Start with
> [Using Global Memory](GLOBAL_MEMORY_GUIDE.md). This document defines the underlying storage,
> epistemic, temporal, concurrency, and sharing contracts.

Status: implementation contract for phases 0-5 (sharing scope limited as described below).

## Scope

Global Memory is a user-local knowledge system shared by every repository and MCP session for
the same user. It preserves two user-visible layers:

- `raw/`: immutable, content-addressed source objects.
- `wiki/`: Markdown materialized from committed wiki revisions.

The internal `_global_memory.db` is a hidden SQLite graph, full-text index, revision journal,
and coordination database. It is not an indexed code project and must never appear in
`list_projects`.

The server contains no LLM. Agents read sources, reason, and submit proposals. The server owns
deterministic storage, retrieval, provenance, temporal state, concurrency control, linting,
code-reference validation, and sharing mechanics.

## Non-goals

- Implicitly injecting memory into every session.
- Treating retrieval frequency, wiki links, or graph centrality as evidence.
- Automatically deciding that a contested claim is true.
- Last-write-wins updates.
- Fetching remote source content inside the MCP server.
- Phase 5 ACLs or a general hosted database backend.

## Storage layout

`CBM_MEMORY_HOME` overrides the platform data-directory default.

```text
<memory-home>/
  raw/objects/<sha256-prefix>/<sha256>.<extension>
  wiki/<kind>/<slug>.md
  _global_memory.db
  export/memory-export.json
```

Raw source bytes are canonical and immutable. Committed `WikiRevision` rows are the
transactional canonical wiki history. Markdown files are recoverable materialized views.

## Epistemic model

The graph distinguishes `MemorySource`, `WikiPage`, `WikiRevision`, `Claim`, `Decision`,
`Experience`, `Preference`, `Activity`, and `CodeRef`.

Claims have an explicit kind (`fact`, `inference`, `hypothesis`, or `recommendation`), scope,
status, valid time, recorded time, review time, and volatility. Decisions record alternatives,
assumptions, applicability and invalidation conditions, expected/actual outcomes, review time,
and exit criteria. Experiences record their context, environment, observation, outcome, sample
size, generalization limits, and failure signals.

Preferences record a scoped user or team choice with rationale and context. They influence
applicability, but are never promoted to facts merely because they are repeatedly reused.

Core relationship types are `ASSERTS`, `SUPPORTED_BY`, `CONTRADICTS`, `SUPERSEDES`,
`LINKS_TO`, `APPLIES_TO`, `DECIDED_USING`, `OUTCOME`, `USED`, `GENERATED`, and
`REVISION_OF`.

The following invariants are mandatory:

1. A decision is not a fact.
2. An experience cannot be generalized without scope.
3. A wiki page cannot independently support its own claims.
4. Sources with the same provenance lineage count as one independent source.
5. Usage, repetition, and centrality never increase truth status.
6. New knowledge supersedes, contests, or retracts old knowledge; it does not destroy history.
7. `stale` means revalidation is required, not that a claim is false.

## Retrieval contract

Retrieval is applicability-first. It does not require searching for an opposite view. It finds
the best candidate memory, compares applicability and invalidation conditions with current
context, then selects one route:

- `reuse`: conditions match and evidence is current.
- `verify`: a bounded fact or freshness check is required.
- `experiment`: uncertainty exists but a reversible trial is available.
- `deliberate`: uncertainty and impact are high; alternatives and counter-evidence are useful.
- `abstain`: stale or unresolved evidence prevents a safe recommendation.

Every response includes the memory snapshot epoch, temporal/conflict warnings, applicability
matches/mismatches/unknowns, evidence lineage, and when available verification, success,
failure, and rollback criteria.

For multi-result searches, mismatched candidates remain visible for inspection but do not veto a
usable candidate. The top-level route is derived from the highest-ranked non-mismatched candidate;
aggregate warnings may still describe lower-ranked results. This keeps counter-evidence available
without turning “consider the opposite” into a mandatory tax on straightforward, current facts.

## Temporal contract

Claims carry both world-valid time (`valid_from`, `valid_to`) and knowledge-recorded time
(`recorded_from`, `recorded_to`). Updates append a new claim/revision and close the old recorded
interval. Current, historical-valid, and historical-known queries must be possible.

Dirty/review triggers include review deadlines, new source revisions, changed API/schema files,
dependency-major changes, changed or deleted code references, and changed decision assumptions.

## Concurrency contract

Readers use independent read-only connections and return a `snapshot_epoch`. Writers are short,
serialized WAL transactions. LLM reasoning never occurs inside a transaction.

Wiki changes follow `read -> propose -> commit -> materialize`. Proposals include a base epoch,
expected entity revisions, agent/session identity, and operations. Commits require an idempotent
operation ID. Same-page or same-claim conflicts are rejected; last-write-wins is forbidden.

The commit records revisions, graph mutations, activity, an outbox materialization entry, and a
new global epoch atomically. A worker writes Markdown through temp-file + atomic rename and can
recover pending outbox entries after a crash.

## MCP surface

- `memory_ingest`: add/deduplicate immutable raw sources and return related memory candidates.
- `memory_query`: search/get/overview/neighbors/path/timeline/as-of and applicability routing.
- `memory_status`: read-only epoch, entity, maintenance, CodeRef, and projection counters.
- `memory_propose`: create revision-safe graph/wiki operations.
- `memory_commit`: commit or reject a proposal with epoch/revision/idempotency checks.
- `memory_lint`: epistemic, temporal, bias, graph, materialization, and code-reference health.
- `memory_export`: export a deterministic portable bundle.
- `memory_import`: import with an explicit merge policy.
- `memory_sync`: initialize/status/pull/push Git sync and optionally configure a GitHub remote.

`query_graph` and `get_graph_schema` accept `graph="memory"`; code and missed graphs continue to
require a project while the memory graph does not.

## Agent instruction contract

Installed agent guidance treats Global Memory as conditional, user-global context rather than an
automatic prompt dependency. Agents query it only when prior cross-project facts, decisions, or
experiences are materially relevant, pass current repository/task constraints as context, and
inspect route, applicability, freshness, evidence lineage, and conflicts before reuse. An opposing
view is not searched for by default; verification effort follows the returned route and impact.

Global writes require authorization from the current task and must remain durable, scoped, and
auditable. Repository-specific details and ADRs stay local unless explicitly promoted. Export,
import, and synchronization are explicit operations because bundles include raw source bytes and
may contain sensitive data.

## Code graph integration

Memory stores symbolic `CodeRef` values (project, qualified name, file, and optional commit/tree
hash), never raw node IDs from a project database.

`detect_changes` is observational: it reports changed files and impacted symbols and returns
`global_memory_updated: false`. It does not open, dirty, or revise Global Memory. This keeps a
diagnostic query from becoming an implicit durable write.

After a repository index completes, the indexer publishes an opaque graph generation only after
file hashes, coverage metadata, and FTS are durable. Replacements are built in a sibling staging
database and atomically installed only after the completion marker and checkpoint succeed. A
failed rebuild therefore leaves the previous completed database at its published path. Long-running
MCP readers keep serving that snapshot while a replacement is being built, then reopen after the
completed file is installed. CodeRef validation runs against that completed graph. It updates only references
whose resolved/missing result actually changed; a no-op reindex therefore does not create a Memory
epoch or CodeRef revision. Connected memory is marked for review only when validation detects a
real resolution change.

Repository ADRs remain repository-scoped unless explicitly promoted through a memory proposal.

## Maintenance

Lint covers unsupported facts, inference/fact confusion, single-lineage support, unresolved
contradictions, stale claims, temporal overlap, broken links, orphan pages, duplicate pages,
context-free experiences, decisions without alternatives or review criteria, circular/self
support, retrieval concentration, single-agent dominance, dirty code references, pending
materialization, and conflicting proposals.

Frequently reused or high-impact memory receives higher audit priority, never higher truth
weight.

Projection performance is measured rather than assumed. See
[Global Memory Performance](GLOBAL_MEMORY_PERFORMANCE.md) for the opt-in scaling probe, current
baseline, and the thresholds that would trigger an incremental-projection redesign.

## Sharing scope (Phase 5)

Phase 5 implements only:

1. Deterministic export/import.
2. Git synchronization, with optional GitHub remote configuration through existing `git`/`gh`
   credentials.
3. Explicit merge policies: `reject`, `keep_local`, `keep_remote`, and `newest` (the latter is
   allowed only for disjoint or unchanged-base revisions; semantic conflicts remain proposals).

Bundles contain the raw source bytes by design. Before configuring a remote, users must treat the
bundle as potentially sensitive and choose a repository with suitable visibility. Credentials are
left to Git credential helpers or SSH agents and are never embedded in Memory metadata or logs.
Imports validate raw hashes, canonical relation/revision integrity, merge logical rows in a short
transaction, rebuild the graph/FTS projection, and materialize current wiki revisions through the
local outbox. The live database file is never replaced.

Remote ACLs, multi-user authorization, and a hosted database service are out of scope.
