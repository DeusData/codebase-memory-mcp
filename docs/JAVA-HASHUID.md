# Java HashUID (TrackerV2-compatible identity)

## Status

Implemented and verified. Java `Class`, `Interface`, `Enum`, `Method` and
`Constructor` nodes carry a `hashuid` property whose value is byte-identical to
the one the TrackerV2 scanner (`osana`) produces for the same declaration.

This document is the operator-facing reference: what the field is, which nodes
have it, which do not and why, how to query it, and how cbm deliberately differs
from TrackerV2.

## What it is

TrackerV2 identifies a Java declaration by hashing eight fields joined by `|`:

```
name | signature | path | condition | logical_module | type_id |
language_type_id | duplicate_fingerprint
```

and taking the MD5 of that UTF-8 string. cbm reproduces the bytes exactly, so
the same declaration has the same identity on both sides and a node can be
followed across tools and across revisions.

Each tracked node carries the identity in **dedicated node columns** (index
format v2; before that they were keys inside `properties`):

| Column | Meaning |
|---|---|
| `hashuid` | 32-char lowercase hex MD5. The identity. |
| `canonical_signature` | `name(Type,Type)` — the signature half of the input |
| `condition` | always empty for Java, but the column exists so the identity decomposes field for field |
| `logical_module` | TrackerV2's enclosing-container chain of simple names |
| `duplicate_fingerprint` | non-empty only for declarations sharing a key in one file |
| `type_id` | `2` method, `3` constructor, `20` class, `22` enum, `23` interface |
| `language_type_id` | `2` for Java |

`name` and `file_path` — the other two of the eight hashed fields — are the
node's own columns, so all nine values behind the digest are selectable without
touching JSON. `hashInput` (the exact hashed string) stays a `properties` key:
it is a cbm-only diagnostic, not one of the nine.

Every one of the seven is NULL on a node without an identity, which is the
majority of a Java graph. A unique index (`idx_nodes_hashuid`) over
`(project, hashuid)` enforces that no two nodes of a project share a digest;
NULLs are exempt, which is what lets the untracked majority coexist.

The move from JSON keys to columns is an index-format bump: a v1 database has
no such columns, so cbm rejects it at open and rebuilds the project on the next
index run.

## Which nodes have it

| Node | Identity | Why |
|---|---|---|
| `Class` | yes | type_id 20 |
| `Interface` | yes | type_id 23 |
| `Enum` | yes | type_id 22 |
| `Method` (with a body) | yes | type_id 2 |
| `Constructor` (label `Method`) | yes | type_id 3, detected by "no return type and the name equals the enclosing class's simple name" |
| `Method` **without** a body | **yes** — deliberate superset | interface / abstract / native declarations are real, referencable entities. TrackerV2 skips them. Identify them with `hasBody:false`. See "Deliberate differences" below. |
| `record` declaration | no | not in TrackerV2's type registry, and it does not scope its members |
| `@interface` (annotation type) | no | not in TrackerV2's type registry — but it **does** scope its members, see below |
| anonymous class scope (`new T() { ... }`) | no | not in TrackerV2's registry, but its **members** are |
| `Field`, `Variable`, `File`, `Folder`, `Route`, `Decorator`, `Branch`, `Project` | no | cbm-only node kinds; TrackerV2 has no notion of them |

Nodes that legitimately have no identity are the majority (about 62% of a Java
graph) and are not an error: they are simply outside TrackerV2's model.

## Rules that are easy to get wrong

- **Empty fields keep their separators.** `condition` is always empty for Java
  and `duplicateFingerprint` is usually empty, so the input contains `||` and a
  trailing `|`. Dropping either changes the digest.
- **`logical_module` is a chain of container simple names**, not a path: a
  method of a top-level class reports the class name, the top-level class itself
  reports nothing, and TrackerV2 counts method and constructor declarations as
  containers (so a member of an anonymous class created inside
  `GitClient.iterateFileContents` reports
  `GitClient.iterateFileContents.$AC_Iterator`).
- **`@interface` scopes its members; `record` does not.** TrackerV2's container
  set is `class_declaration | interface_declaration | enum_declaration |
  annotation_type_declaration | method_declaration | constructor_declaration`.
  An enum declared inside an annotation type therefore reports the annotation's
  name as its `logical_module` (`TestAnnotation.Level` → module
  `TestAnnotation`), while a member of a `record` reports only the enclosing
  class. Neither type carries an identity of its own.
- **`duplicate_fingerprint` is per file and position dependent.** Declarations
  sharing the first seven fields get `sha256(declaration text) + index`, indexed
  by `(start_line, end_line)`. Inserting a new colliding declaration earlier in
  a file therefore *changes the identity of the later ones*. This matches
  TrackerV2 and is unavoidable while keeping byte-compatibility.
- **Anonymous class scopes are disambiguated in the QN, not in the identity.**
  The scope QN is `$AC_<Type>@<byte offset>` so that several anonymous classes
  of one type in one scope stay distinct nodes; the scope's `name` — which is
  what feeds `logical_module` — stays `$AC_<Type>`.

## How to query

The dedicated MCP tool — prefer this over hand-writing a query:

```
get_by_hashuid(project, hashuid, include_identity?)
```

`hashuid` is the 32-character hexadecimal identity. Uppercase is accepted and
re-canonicalized, because the store holds lowercase and a verbatim compare
would turn a copy-pasted correct digest into a silent miss. The identity is
unique per project, so the answer is one node and never a list.

`include_identity: true` also returns the identity block:
`canonical_signature`, `condition`, `logical_module`, `duplicate_fingerprint`,
`type_id`, `language_type_id` (all read from the columns) plus `hash_input` and
`has_body` (the two values that still live in `properties`). `hash_input` is the
exact hashed string, which is the fastest way to see *which* of the eight fields
differs when a digest does not line up with TrackerV2. Empty-but-present values
are rendered as `-` by the compact emitter, so `condition` reads as `-` on every
Java node.

A miss is a result, not an error: it returns `found: false` with a hint, since
identities are stamped by the full index path only and a project indexed
through the closure-delta route legitimately has none.

SQL over the graph database:

```sql
SELECT name, qualified_name
FROM nodes
WHERE project = 'my-project'
  AND hashuid = 'b9a50aa234cc33ce563eb182efdc8091';

-- declarations that have no body (TrackerV2 would not know them)
SELECT count(*) FROM nodes
WHERE hashuid IS NOT NULL
  AND json_extract(properties, '$.hasBody') IS 0;

-- the whole identity, decomposed exactly as TrackerV2 hashes it
SELECT name, file_path, canonical_signature, condition, logical_module,
       type_id, language_type_id, duplicate_fingerprint, hashuid
FROM nodes
WHERE project = 'my-project' AND hashuid IS NOT NULL;
```

Cypher, through `query_graph`:

```cypher
MATCH (n) WHERE n.hashuid = 'b9a50aa234cc33ce563eb182efdc8091' RETURN n.name, n.qualified_name
MATCH (n:Method) WHERE n.hashuid IS NOT NULL RETURN count(n)
```

## Deliberate differences from TrackerV2

1. **Body-less declarations get an identity.** TrackerV2 emits identities only
   for declarations that contain a body. cbm stamps interface, abstract and
   native methods as well, because they are referencable entities that the
   graph tracks (and that inheritance/override resolution needs). They are one
   filter away from a strict comparison: `hasBody:false`. On the reference
   corpus this is 289 identities.
2. **`int[]...` varargs.** TrackerV2's signature extractor accepts only a
   `type_identifier` child inside a spread parameter, so a varargs parameter
   whose type node is an `array_type` is silently dropped and its signature
   loses that argument (`snapshot(int,int[]...)` becomes `snapshot(int)` in its
   output). cbm keeps the parameter. This is an upstream bug, not a cbm choice.

Everything else matches byte for byte: on the reference corpus (649 Java files,
commit `b6016d94`) 3,221 of osana's 3,223 identities are reproduced exactly, and
the two exceptions are the varargs cases above.

## Scope and limits

- **Java only.** No other language has an identity. The pass gates every
  candidate on the file it was extracted from (`cbm_language_for_filename()`
  over the node's `file_path`), so another language's `Class` / `Interface` /
  `Enum` node — which carries no `javaTypeKind` and would otherwise pass the
  type test — is left unstamped, as are synthetic paths such as
  `<python-builtins>`. Methods were already Java-only via the `hasBody`
  marker; the language gate covers the type half as well.
- **Full index only.** The closure-delta incremental route does not stamp
  identities, because its buffer holds proxy nodes whose enclosing containers
  may be absent and a truncated `logical_module` would be silently wrong.
- **The identity set is not the node set.** About 62% of nodes are outside
  TrackerV2's model (`File`, `Folder`, `Field`, `Variable`, `Route`, …) and have
  no identity by design. `qualified_name` remains the storage key for every
  node; `hashuid` is a second, tool-independent business key for the tracked
  subset.

## Reproducing the comparison

1. Build and index the snapshot:

   ```
   make -f Makefile.cbm cbm BUILD_DIR=<build-dir>
   codebase-memory-mcp cli index_repository --repo-path <java-snapshot> --name ctv2 --mode full
   ```

2. Compare against the TrackerV2 scanner output for the same revision with
   `align_hashuid.py` (in the `fdse` workspace's `.cbm-tools/`), which reports
   identical / cbm-only / osana-only identities with the first differing field
   named for every mismatch.
