# Runtime Trace Observation Model

## Status and recommendation

This document is an architecture contract, not an implementation. It recommends a
durable, versioned runtime-observation sidecar whose publications can be read alongside
a pinned static index generation. Runtime observations MUST NOT patch static `CALLS`
edges, fabricate static nodes, or appear as persistent edges in the default graph.

The model intentionally separates facts observed at runtime from facts derived from
source. A caller may explicitly request a `RUNTIME_CALL` overlay; existing static
search, architecture, and default trace behavior remain unchanged.

This document-only change does not alter the runtime wire version, runtime schema
version, runtime semantic version, runtime artifact version, static semantic index
version, or artifact schema version. In particular, the static semantic index version
remains 3.

## Scope

The model defines:

- stable identities for spans, producer contributions, observations, endpoints, and
  composite publications;
- deterministic ingestion, aggregation, publication, and replay semantics;
- generation-scoped resolution from runtime endpoints to static symbols;
- an opt-in runtime call overlay without changing the stored static graph;
- concurrency, retry, conflict, recovery, privacy, and cardinality boundaries; and
- six bounded implementation packages that can be reviewed and shipped separately.

## Non-goals

This design does not:

- infer new static `CALLS` edges or change confidence on existing ones;
- create placeholder nodes in the static graph for runtime-only identities;
- persist runtime edges in the generic graph edge table;
- make runtime data visible to existing graph tools unless an overlay is requested;
- define distributed tracing transport, sampling policy, or collector deployment;
- promise complete traces, causal proof from timing alone, or source-line attribution;
- store arbitrary span payloads, headers, request bodies, query strings, or secrets;
- average percentile summaries or treat a producer's p99 as a mergeable percentile;
- provide a compatibility promise for a future mutation API before its versioned
  contract exists; or
- change any runtime or static version merely by adding this document.

## Current surface

The current `ingest_traces` surface requires `project` and `traces[]`. Each trace item
permits `caller`, `callee`, and `count`, but those item fields are not currently
individually required. Its handler counts the supplied records and returns
`status="accepted"`, `traces_received`, and an unimplemented note; this is not an
accepted durable mutation, and the handler does not open or mutate a store.

The existing trace helper surface can extract `service.name`, HTTP method, HTTP path,
HTTP status, span kind, duration, URL path, and p99. Verified span-kind values are:

- `1`: internal;
- `2`: server; and
- `3`: client.

Generic graph edges are unique by source, target, type, and the local-name
discriminator. Generic edge upsert merges properties with `json_patch`. Those rules are
appropriate for the static graph but are not the contribution or aggregation rules in
this document. The store already exposes `BEGIN IMMEDIATE`, `COMMIT`, and `ROLLBACK`.
Static publication already uses opaque generations and request-scoped reads.

## Normative terms

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative. "Canonical JSON" means UTF-8
JSON with a fixed schema, lexicographically sorted object keys, arrays in their
specified order, no insignificant whitespace, and integers rather than locale- or
platform-dependent numeric strings. Hashes are computed over those canonical bytes.

## Identity layers

The following identities are different and MUST NOT be substituted for one another.

### Trace and span identity

A span is identified within one producer namespace by:

```text
(project, producer_id, producer_epoch, trace_id, span_id)
```

`trace_id` and `span_id` are opaque byte strings rendered in one canonical lowercase
hex form at the boundary. `producer_epoch` is part of span identity so a producer can
reuse trace or span identifiers in a later epoch without collision. The tuple identifies
an input span for diagnostics and deduplication; it does not identify a static node or
an aggregate.

### Producer contribution identity

Every accepted batch has a producer-stable identifier:

```text
contribution_id = H(canonical JSON {
  producer_id,
  producer_epoch,
  source_batch_id
})
```

`producer_id` names the authenticated logical producer, not a transient process.
`producer_epoch` changes only when that producer intentionally starts a new identity
domain. `source_batch_id` is stable across delivery retries. The resulting
`contribution_id` is scoped by project.

An accepted contribution is immutable. Retrying the same `(project, producer_id,
contribution_id)` with the same canonical payload hash is an idempotent success and
returns the original disposition. Retrying it with different canonical bytes is a
conflict and MUST be rejected without mutation. A producer MUST NOT reuse another
producer's namespace, and the service MUST NOT silently mint a replacement ID to turn
a conflict into a second contribution.

The payload hash covers the canonical normalized allowlisted contribution map together
with its pinned wire and runtime semantic version fields. It does not cover original
envelope ordering, unsupported fields, discarded fields, or raw payload bytes.

### Endpoint identity

Every endpoint has an explicit tag and a canonical key.

1. `symbol`: a source-derived symbol reference `(project, qualified_name)`. This stable
   endpoint identity deliberately excludes a static generation. A separate resolution
   record maps it within one named static generation and resolver version. A later
   generation MUST resolve again and MUST NOT inherit a node identifier from an older
   generation.
2. `runtime`: a runtime-only endpoint derived from an allowlisted protocol identity,
   such as canonical service name plus normalized HTTP route and method. It is stored
   only in the sidecar and never creates a static node.
3. `unknown`: an opaque, scoped endpoint used when the allowlisted evidence cannot
   identify either a symbol or a runtime service endpoint. Unknown keys MUST be stable
   only inside their documented scope (for example, producer plus trace) and MUST NOT
   collapse unrelated missing values into one global endpoint.

Endpoint canonicalization is versioned by `runtime_semantic_version`. All strings are
Unicode-normalized, methods and protocol tokens use their specified canonical case,
and absent values are distinct from empty values. Raw URL queries, fragments, and
credentials are excluded.

### Observation identity

An observation describes one directed runtime perspective, not one delivery. Its
canonical key is:

```text
observation_key = H(canonical JSON {
  runtime_semantic_version,
  project,
  protocol,
  perspective,
  source_endpoint_key,
  target_endpoint_key,
  operation_key
})
```

The key excludes counters, timestamps, duration summaries, status summaries,
contribution IDs, and static database row IDs. `operation_key` contains only the
versioned allowlisted operation identity, such as canonical HTTP method and normalized
route template. Consequently, identical observations from multiple producers converge
while semantically different directions or operations remain distinct.

### Composite publication identity

A reader-visible snapshot is identified by the atomic pair:

```text
{ static_generation, runtime_generation }
```

The pair is the head identity, but its halves are independent immutable publications.
A runtime generation identifies runtime contributions and aggregates without embedding
a static generation. Generation-scoped symbol resolutions and overlay projections are
keyed by the pair plus resolver version. Neither half may be silently advanced while a
request is using the pair.

## Protocol and perspective mapping

Ingestion parses and normalizes only versioned, allowlisted span fields into the
contribution, then derives a perspective from that evidence. The raw span payload is
not durably preserved. The HTTP mapper uses the existing service-name, HTTP method,
HTTP path or URL path, status, span-kind, and duration helpers. The route field is
normalized as a route template when the source provides one; a raw high-cardinality URL
path MUST NOT be promoted to a template by guesswork.

For the verified span kinds:

- `client` (`3`) represents an outbound attempt. The source is the resolved local
  symbol when available, otherwise a tagged local runtime or scoped unknown endpoint.
  The target is the allowlisted remote service/HTTP endpoint or a scoped unknown
  endpoint. The perspective token is `outbound`.
- `server` (`2`) represents an inbound handling attempt. The source is an allowlisted
  peer/runtime endpoint or a scoped unknown endpoint. The target is the resolved local
  handler when available, otherwise the local service/HTTP runtime endpoint. The
  perspective token is `inbound`.
- `internal` (`1`) represents local work. A directed call projection is produced only
  when explicit parent/child evidence supplies two endpoints. Otherwise the
  observation remains queryable as internal activity but does not become a
  `RUNTIME_CALL`. The perspective token is `internal`.

Unsupported or absent span kinds are retained only as normalized, allowlisted non-call
observation metadata if policy allows; unsupported fields and raw payload bytes are
discarded. They MUST NOT be guessed into one of the three perspectives. Status and
duration contribute to summaries but never to observation identity.

Symbol resolution is best-effort and generation-scoped. Ambiguous matches remain
tagged runtime or unknown endpoints. Resolution MUST fail closed: it must not choose a
symbol by filename substring, short-name coincidence, timing, or insertion order.

## Sidecar model

The durable sidecar is conceptually separate from the static graph store even if an
implementation initially shares the same database file. It contains these logical
relations:

### `runtime_endpoints`

Stores the endpoint tag, canonical endpoint key, and canonical allowlisted identity.
For a `symbol` endpoint, the durable identity contains project plus qualified name but
no static generation. Generation and static database row IDs belong only to resolution
records or caches, not durable endpoint identity.

### `runtime_contributions`

Stores project, producer ID, contribution ID, canonical payload hash, ingestion state,
and immutable canonical contribution metadata. States include at least `staged` and
`finalized`; an aborted transaction leaves neither state visible.

### `runtime_observation_contributions`

Maps `(contribution_id, observation_key)` to that contribution's canonical summary.
The map entry is immutable after finalization. Replacing it requires a new contribution
identity.

### `runtime_aggregates`

Stores the deterministic fold of finalized contribution-map entries for each
observation key and runtime generation. It is derived state and can be rebuilt from
finalized contributions.

### `runtime_resolutions`

Maps a stable runtime or unknown endpoint key to an optional stable SymbolEndpoint
`(project, qualified_name)` plus its current static node/cache result, unresolved
result, or ambiguity result. Each record is keyed by project, runtime generation,
static generation, stable endpoint key, and resolver version. Resolution records are
derived caches, not endpoint or observation identity, and a record from one static
generation MUST NOT be reused for another.

### `runtime_publications`

Stores immutable runtime generations independently of static generations. A separate
atomic per-project composite head pairs one current runtime generation with one current
static generation. Publication metadata includes the relevant runtime versions; the
composite head records the static index/artifact versions used for compatibility
checks.

No table above is a static graph node or edge table. The implementation MUST NOT reuse
generic `json_patch` edge upsert for contribution merging.

## Deterministic aggregation

Before immutable summaries are stored, one contribution is normalized deterministically:

1. canonicalize only allowlisted fields and compute the exact span identity
   `(project, producer_id, producer_epoch, trace_id, span_id)`;
2. deduplicate records with the same span identity and identical normalized bytes;
3. reject the entire contribution if one span identity has different normalized bytes;
4. group the remaining unique normalized records by `observation_key`;
5. after keyed exact-span deduplication/set union supplies idempotence, fold integer
   counts and duration sums, fixed status buckets, and fixed-schema histogram bins over
   the unique normalized span map using associative and commutative exact addition; and
6. sort keys and canonical-serialize the contribution map before hashing or storage.

The normalized contribution map and its immutable summaries MUST be byte-identical for
every input ordering and worker partitioning of the same unique normalized span set.
Discarded or unsupported fields cannot change the payload hash.

For each observation, aggregation is a map union keyed by contribution identity:

```text
aggregate(observation) = fold(sorted union {
  contribution_id -> canonical contribution summary
})
```

Union of identical key/value entries is idempotent. A differing value under the same
key is a conflict, not a winner selection. Map union is associative and commutative;
sorting by contribution ID before serialization makes the resulting bytes independent
of producer, retry, transaction, or worker order.

Canonical contribution summaries MAY contain integer count, integer duration sum in a
fixed unit, fixed-schema status buckets, and a mergeable duration distribution whose
encoding is pinned by `runtime_semantic_version`. Aggregate totals are exact integer
sums over the contribution map. Derived display values are computed after the fold.

Percentiles are not additive. A batch p99 MUST NOT be averaged, weighted-averaged, or
selected as the aggregate p99. The existing p99 helper may summarize a single raw batch
for diagnostics, but a published cross-contribution p99 requires a versioned mergeable
distribution (for example fixed histogram boundaries). Without such a distribution,
the aggregate p99 is unavailable.

Canonical aggregate JSON MUST be byte-identical for every permutation and
parenthesization of the same finalized contribution set.

## Ingestion and publication transaction

Full wire parsing, canonicalization, payload hashing, and every validation that does
not depend on mutable project state happen before acquiring the publication lock.
Publication is then serialized by one shared per-project publication lock used by both
static and runtime publishers. An implementation MUST NOT introduce an independent
runtime lock that permits lost updates to either half of the composite head.

Within that lock, a runtime publisher:

1. begins a write transaction with `BEGIN IMMEDIATE`;
2. reads and pins the current composite head and validates compatibility versions;
3. performs state-dependent authorization, quota, producer identity, deduplication,
   contribution conflict, and expected-head checks;
4. stages all endpoints, immutable contribution-map entries, and rebuilt aggregates;
5. allocates an opaque runtime generation independent of the pinned static generation;
6. durably finalizes every contribution in the publication;
7. writes the immutable runtime publication and atomically changes only the runtime
   half of the composite head, preserving its pinned static generation;
8. commits all changes with `COMMIT`; and
9. releases the per-project publication lock.

Every validation or write failure before commit uses `ROLLBACK`. There is no visible
partial contribution, aggregate, generation, or head. Finalization and head movement
are in the same transaction, so a success response is sent only after durable commit.

Static publication follows the same lock discipline and atomically changes only the
static half of the composite head, preserving its pinned runtime generation. It does
not create a new runtime generation merely because source was reindexed. Resolution
records for the new `{static_generation, runtime_generation}` pair are recomputed or
materialized under their pair-and-resolver-version key; records from the old static
generation are never treated as resolutions for the new one.

## Reader contract

A request that opts into runtime data pins one composite head at request start. Every
page, endpoint resolution, aggregate, and overlay edge in that request is read from
that pair. Default requests pin only their existing static request scope and do not pay
for or expose the overlay.

Pagination cursors encode the project, composite head, query shape, overlay mode, and
runtime semantic version. If the current head differs, a subsequent request with that
cursor returns a stale-cursor error; it never resumes against mixed generations.
Readers MAY explicitly request a still-retained historical composite head. Missing or
garbage-collected heads fail explicitly.

## `RUNTIME_CALL` overlay

Runtime call projection is an explicit read option. It presents directed overlay edges
derived from one pinned runtime generation. These edges:

- use the distinct type `RUNTIME_CALL`;
- carry endpoint tags so callers can distinguish symbol, runtime, and unknown ends;
- include deterministic aggregate summaries and the composite head identity;
- never mutate, replace, increase confidence on, or suppress a static `CALLS` edge;
- are omitted from existing search, architecture, and default trace responses unless
  the caller requests the overlay; and
- are regenerated when either half of the composite publication changes.

If a projected runtime call has symbol endpoints, it references their qualified-name
identity within the pinned static generation. Runtime-only and unknown endpoints remain
sidecar objects and are not fabricated as graph nodes.

## Version boundaries

Compatibility is divided deliberately:

- `wire_version`: request and response envelope, field encodings, and retry result;
- `runtime_schema_version`: sidecar tables, indexes, and migration compatibility;
- `runtime_semantic_version`: canonical endpoint/observation keys, mappings, and
  aggregation meaning;
- `runtime_artifact_version`: exported/imported sidecar artifact layout;
- `static_semantic_index_version`: source-derived graph semantics; and
- `static_artifact_schema_version`: static artifact representation.

A change bumps only the boundary it changes. A runtime semantic change can invalidate
runtime generations without changing static semantic index version. A storage-only
migration can change runtime schema version without changing observation identity.
Import rejects incompatible artifact versions before mutation. This document changes
none of these versions.

## Security, privacy, and cardinality

Ingestion is project-authorized and producer-authenticated. Producer IDs, contribution
IDs, and project scope are server-validated; caller-provided project or producer fields
do not grant access. Reads apply the same project authorization to runtime and static
data.

Only allowlisted attributes enter canonical identities or summaries. Credentials,
authorization data, cookies, request/response bodies, query strings, fragments,
arbitrary baggage, and raw exception text are rejected or discarded before durable
storage. Logs and errors use opaque IDs and counts, not rejected payloads.

The service enforces bounded request bytes, records, endpoints, observations, attribute
lengths, route-template segments, status buckets, and contribution-map growth. Raw URL
paths are not accepted as unbounded route labels. Unknown endpoints are scoped to avoid
both accidental global collapse and attacker-controlled global cardinality. Quotas and
retention apply per project and producer. Limit failure is all-or-nothing and does not
publish a truncated contribution unless a future wire version explicitly defines that
behavior.

## Failure and recovery matrix

| Condition | Required disposition | Visible state |
| --- | --- | --- |
| Same contribution ID, same payload hash | Idempotent success; return prior result | Unchanged |
| Same contribution ID, different payload hash | Conflict; reject entire request | Unchanged |
| Unauthorized project or producer | Reject before staging | Unchanged |
| Invalid/unsupported wire or semantic version | Reject before staging | Unchanged |
| Invalid endpoint, attribute, or cardinality limit | Reject entire contribution | Unchanged |
| Ambiguous symbol resolution | Keep runtime/unknown tag; do not guess | Valid sidecar observation only |
| Client-supplied expected composite head differs from the head read under lock | Reject as stale; retry from the newly read head | Unchanged |
| Failure while staging or aggregating | `ROLLBACK` | No partial state |
| Crash before commit | Database recovery rolls back the transaction | Prior head remains |
| Commit succeeds but response is lost | Producer retry resolves idempotently | One finalized contribution |
| Derived aggregate missing or corrupt | Rebuild from finalized contribution map before publish | Prior valid head remains |
| Cursor references a different or removed head | Return stale/missing cursor error | No mixed-generation read |
| Unsupported span kind | Retain only normalized, allowlisted non-call observation metadata; discard unsupported fields and raw payload bytes | No guessed call overlay |
| No mergeable duration distribution | Publish counts/sums; report p99 unavailable | No averaged p99 |

## Alternatives considered

### Patch static `CALLS` edges

Benefit: existing graph queries would see runtime evidence with no overlay option.

Trade-offs: it conflates observed and source-derived semantics, lets sampled or hostile
telemetry alter the static graph, cannot represent runtime-only endpoints honestly,
couples runtime retention to static indexing, and makes rollback and provenance
ambiguous. It is rejected.

### Fabricate static nodes for runtime services and unknown peers

Benefit: all endpoints would fit the existing graph node/edge shape.

Trade-offs: fabricated nodes look source-derived, create unstable identities and high
cardinality, collide with future symbol resolution, and pollute architecture/search
results. It is rejected.

### Store persistent runtime edges in the default graph

Benefit: generic edge uniqueness and `json_patch` upsert are already available.

Trade-offs: generic uniqueness has no immutable producer-contribution dimension;
`json_patch` is neither the required commutative map union nor a conflict detector;
default readers would unexpectedly mix generations and data classes. It is rejected.

### Keep runtime observations entirely ephemeral

Benefit: minimal schema, retention, and migration burden; no durable sensitive data.

Trade-offs: retries cannot be made idempotent across restarts, results are not
reproducible or pageable, multi-producer aggregation is unstable, and a reader cannot
pin a coherent composite publication. It is rejected as the primary model, though
ephemeral pre-validation remains useful.

### Durable versioned sidecar with an opt-in overlay (selected)

Benefits: preserves static semantics, gives runtime-only endpoints an honest identity,
supports deterministic replay and aggregation, isolates retention and authorization,
and provides atomic cross-generation reads.

Trade-offs: adds schema and version boundaries, a shared publication lock, endpoint
resolution, aggregate rebuilds, overlay-aware APIs, and explicit operational limits.
These costs are accepted because they make provenance, concurrency, failure recovery,
and compatibility testable rather than implicit.

## Bounded implementation packages

Implementation remains deferred. The following packages are intentionally ordered and
bounded; each requires its own reproduce-first tests and review.

1. **Versioned wire validation and canonicalization.** Define authenticated producer
   identity and epoch, contribution ID, exact span-identity deduplication, conflicting
   duplicate rejection, canonical contribution maps and payload hashes, allowlisted
   HTTP/span mapping, limits, and dry-run validation. No store mutation.
2. **Sidecar schema and immutable contributions.** Add runtime schema migrations,
   endpoints, contribution records, observation-contribution maps, idempotent retry,
   conflict rejection, and transactional rollback. No publication head or overlay.
3. **Deterministic aggregation.** Add canonical intra-contribution grouping, keyed
   exact-span idempotent set union/deduplication, associative and commutative exact
   integer/status/histogram addition, cross-contribution map-union folds, fixed
   mergeable duration distributions, input/worker/merge-order permutation tests,
   rebuild verification, and explicit unavailable p99 behavior.
4. **Generation-scoped resolution and publication.** Add the shared per-project lock,
   independent runtime generation allocation, durable finalization, pair-and-version
   resolution records, atomic composite head updates that preserve the other half, and
   crash/retry recovery tests.
5. **Pinned read and `RUNTIME_CALL` overlay.** Add opt-in queries, tagged endpoints,
   request-scoped composite pins, stale cursors, historical-head handling, and proofs
   that default static tools remain byte-for-byte unchanged.
6. **Artifact, retention, and operational hardening.** Add versioned runtime artifact
   import/export, quota/retention policy, authorization audits, corruption rebuild,
   migration/rollback tests, metrics, and multi-producer concurrency tests.

No package may silently absorb a later package's public surface or persistence model.

## Acceptance invariants

An implementation of this contract is acceptable only if all of these hold:

1. Static nodes and `CALLS` edges are byte-identical with runtime ingestion enabled or
   disabled for the same source input.
2. Existing search, architecture, and default trace responses exclude runtime data.
3. Runtime data appears only through an explicit, versioned overlay request.
4. Same producer contribution plus same payload is idempotent across retries and
   restarts; a different payload under that identity is a no-mutation conflict.
5. Normalized contribution and aggregate bytes are identical for every input, delivery,
   worker, and merge order of the same unique normalized span set, including identical
   duplicate replay; a conflicting duplicate span identity rejects the whole
   contribution without mutation.
6. Aggregate p99 is derived only from a pinned mergeable distribution and is otherwise
   unavailable; producer p99 values are never averaged.
7. Symbol resolution is qualified-name based, generation-scoped, and fail-closed on
   ambiguity; runtime and unknown endpoints never fabricate static nodes.
8. Runtime generation state contains all of its finalized contributions, endpoints,
   and aggregates, and reader visibility requires the separate atomic composite-head
   update; the transaction exposes both the complete generation and head update, or
   neither.
9. Every overlay read and page uses one immutable
   `{static_generation, runtime_generation}` pair; stale cursors fail explicitly.
10. Static and runtime publication serialize through the same per-project lock; each
    publisher changes only its own half of the composite head and preserves the other,
    and symbol-resolution records are never reused across static generations or
    resolver versions.
11. Unsupported span kinds, missing identities, and limit violations never become
    guessed calls or partial publications.
12. Authorization, privacy filtering, quotas, and retention are enforced before data
    becomes durable or reader-visible.
13. Wire, runtime schema, runtime semantic, runtime artifact, static semantic, and
    static artifact versions can change independently according to their boundaries.
14. Documentation-only adoption leaves all six version values and all runtime behavior
    unchanged.
