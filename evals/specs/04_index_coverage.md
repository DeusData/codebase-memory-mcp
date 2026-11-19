# 04 Index-creation coverage

Case files: `cases/common/00_schema_and_status.json`, `cases/common/60_coverage.json`
(both backends). Ground truth: `fixtures/expected/eval-small.json`. Script:
`harness/coverage_check.py`.

"Coverage" here means: of what is on disk and in scope, what reached the graph,
with the right label, file, and relationships; and that out-of-scope material did
not. It is measured as recall against a hand-verified list, never against the
product's own report alone.

## Dimensions and evals

### Files

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| COV-01 | every tier-1 source file (10 Python, 4 Java) is a `File` node with its repository-relative, forward-slash path; `vendor/…` and `generated/…` are not | all present, none of the excluded | 1 |
| COV-02 | docs (`README.md`, `docs/ARCHITECTURE.md`), infra (`Dockerfile`, `docker-compose.yml`), empty `__init__.py` | present | 2 |
| C00-07 | `get_graph_schema` lists `File`, `Function`, `Class`, `Method` | | 1 |

### Symbols

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| COV-04 | 22 Python (name, label, file) triples | each is a row of `MATCH (n) … RETURN n.name, n.label, n.file_path` | 1 |
| COV-05 | 11 Java triples (classes, methods, the test class and method) | same | 1 |
| COV-06/07 | unicode identifier in the unicode file; the 200-character identifier kept whole | | 1 / 2 |
| COV-03 | nothing from ignored files; `compute_total` (only mentioned in README) is not a symbol | count 0 | 1 |

### Edges

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| COV-10..15 | tier-1 CALLS: `format_cents→round_cents`, `checkout→validate`, `checkout→notify`, `main→slugify`, `issue→audit`, `format→roundCents` | each ≥ 1 | 1 |
| COV-16 | `hub` has exactly 12 inbound CALLS; `unused_helper` has 0 | | 1 |
| COV-17 | tier-2 CALLS that need the resolver: `notify→format_cents`, `total→round_cents`, `main→checkout`, `issue→format`, `total→roundCents` | freeze at the pilot | 2 |
| COV-18 | every non-empty source file `DEFINES` something | 0 files without | 2 |
| QG-12/13 (spec 07) | `DEFINES_METHOD` from `OrderService`; `DEFINES` from `app/service.py` | | 1 / 2 |

### Documentation and infrastructure

| Id | Eval | Tier |
|---|---|---|
| COV-19 | Markdown `Section` nodes `Orders`, `Billing`, `Dead code` | 2 |
| COV-20 | compose services `db`, `orders` (`Resource`) and `ORDER_ENV` (`EnvVar`) | 2 |

### Ignore rules

- `.cbmignore` (`vendor/`) and `.gitignore` (`generated/`) are both discovery-time
  exclusions: COV-01/03, SG-22/24, SC-10/11, CS-13/28, S02-14 all assert their
  absence from nodes, search, snippets and the SQLite file.
- S01-02 asserts the index reply lists the excluded `vendor` subtree.

### Coverage tools

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| COV-30/31 | `check_index_coverage paths=[…indexed…]` | all four listed; none flagged (vocabulary frozen at the pilot) | 1 / 2 |
| COV-32 | an ignored path | listed with a reason (ignored/excluded/not indexed) | 1 |
| COV-33 | a nonexistent path | reported absent | 1 |
| COV-34/35 | `scopes` and `path_limit` paging | | 1 |
| COV-36..41 | freshness: edit without re-index → flagged stale; re-index → clean; restore | | 2 |
| COV-42 | `index_status diagnostics=full` has coverage rows | | 1 |
| COV-43 | the `missed` graph is queryable and tiny for this clean fixture (≤ 2 files) | | 1 |
| C00-04/05 | `diagnostics=summary`, `verbose` | run | 2 |

### Ground-truth recall script (COV-44)

`harness/coverage_check.py` reads the whole graph through `query_graph` and prints:

```
files_tier1       14/14  recall=1.000 MUST
files_tier2       10/10  recall=1.000
never_indexed     files=[] symbols=[] MUST
symbols_tier1     34/34  recall=1.000 MUST
symbols_tier2      9/9   recall=1.000
calls_tier1        8/8   recall=1.000 MUST
calls_tier2       11/11  recall=1.000
hub_inbound       12 (expected 12) MUST
dead_code         inbound={'unused_helper': []} MUST
duplicates        ['dup/alpha.py', 'dup/beta.py'] MUST
coverage_ok
```

Pass rule: every `MUST` line at 100% / exact. The tier-2 recalls are the
scorecard's "extractor quality" numbers; a drop between two builds is a
regression even though it does not fail the run.

### Resource limits (COV-45..48)

With `index_max_files` set below the fixture's file count, `index_repository` fails
with `resource_limit_exceeded` and the previous graph keeps serving (hub still has
12 callers). The limit is reset afterwards. See `docs/INDEX_RESOURCE_LIMITS.md`.

### Parity

Coverage must be identical on both backends: spec 09 diffs the `60_coverage`
results. The Neo4j copy is a mirror of the SQLite index, so any difference is a
sync defect, not an extraction one.

## Extending the ground truth

Add the file to `fixtures/make_fixture.py`, the facts to
`fixtures/expected/eval-small.json` (tier 1 only for things a same-file reader can
verify: direct calls, definitions), regenerate with `prepare.py`, and add the
matching `contains_row` to COV-04/05 or a pair step to COV-10..17.
