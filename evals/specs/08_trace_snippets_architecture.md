# 08 trace_path, get_code_snippet, get_file_outline, detect_changes, get_architecture

Case files: `cases/common/40_trace_path.json`, `50_snippets_outline.json`,
`70_detect_changes.json`, `80_architecture.json` (both backends).

## trace_path (TP)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| TP-01/02 | inbound on hub, depth 1 and 3 | `callers_total 12` exact, 12 unique names, all at hop 1; under 5 s | 1 |
| TP-03/04 | outbound on checkout, depth 1 / 2 | validate + notify; + `format_cents` | 1 / 2 |
| TP-05/06 | tests excluded by default; `include_tests` brings `test_checkout` back | | 1 / 2 |
| TP-07/08/09 | Java `issue→audit`, `testIssue→issue` (with tests), `format→roundCents` | | 1 / 2 / 1 |
| TP-10 | fully qualified `function_name` | unambiguous | 1 |
| TP-11 | ambiguous short name `total` (Python and Java) | answers, names candidates | 2 |
| TP-12 | unknown function | clear error | 1 |
| TP-13/14 | `limit 5`; `paging_check.py` over the callers | 5 rows, total 12; pages complete and disjoint | 1 |
| TP-15 | `edge_types IMPORTS` | 0 callers | 1 |
| TP-16/25 | `include_evidence`/`risk_labels` add columns; hidden by default | | 2 |
| TP-17/18/19 | modes `data_flow`, `cross_service`, `run` (compose service → Dockerfile program) | run; `run` names `python` | 2 |
| TP-20/21 | depth 16, depth 0 | rejected | 1 |
| TP-22/23 | alias `trace_call_path`; tree format | | 1 |
| TP-24 | Neo4j: `served_by neo4j`, `remote_queries ≥ 1` | | 1 |

Hub behaviour at scale (5,000 and 269,531 callers) is in spec 11.

## get_code_snippet (CS-01..14)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| CS-01/02 | by qualified name, by short name | the body of `round_cents`; under 5 s | 1 |
| CS-03 | ambiguous short name | answers with a `dup/` candidate | 2 |
| CS-04 | unknown | error or suggestions | 1 |
| CS-05/06 | `outline` of a class; `full` | methods listed; whole class | 1 |
| CS-07 | `include_neighbors` | callee `validate` shown | 2 |
| CS-08/09 | `max_lines 3`; `start_line` | bounded; continues | 2 |
| CS-10/11/12 | Java, unicode, 200-char symbols | | 1 / 1 / 2 |
| CS-13 | symbol from an ignored file | its body never appears | 1 |
| CS-14 | `member_limit 1` | runs | 1 |
| CS-30 | Neo4j: `graph_backend.kind neo4j` | | 1 |

## get_file_outline (CS-20..29)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| CS-20 | `app/service.py` | the six declarations, source order (first row `ValidationError`) | 1 |
| CS-21 | `labels [Function]` | functions only | 1 |
| CS-22/23/24 | `hub/hub.py` | 13 rows; `limit 2` → 2 rows, `total 13`; `offset 2` | 1 |
| CS-25/26 | unicode path; Java file | | 1 |
| CS-27/28/29 | unknown file; ignored file; `../` path | error; error or empty; refused | 1 |

## detect_changes (DC)

Needs the working copy's git history (`prepare.py`).

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| DC-01 | clean tree | `changed_total 0` | 1 |
| DC-02/03 | append a function to `app/money.py` | `scope files` lists the file, `changed_total 1` | 1 |
| DC-04 | `scope impact inbound depth 2` | `impacted_total ≥ 1`, `format_cents` impacted | 2 |
| DC-05 | impact totals | `seed_symbols ≥ 1`, `impacted_total_relation` eq/gte: never a silent zero with seeds present (finding M5b) | 1 |
| DC-06/07 | re-index; `seeds list` | `extra_fn` is a seed | 2 |
| DC-08..12 | `via`, `fixpoint`, `direction both` + `edge_types`, `scope tests`, `seed_filter product` | run; product filter hides `test_checkout` | 2 |
| DC-13..16 | restore, re-index | `changed_total 0`; `extra_fn` gone from the graph | 1 |
| DC-17 | Neo4j kind | | 1 |
| DC-18/19 | `compare_graphs` self vs self; unknown project | no additions; error | 2 / 1 |

## get_architecture, manage_adr, ingest_traces (AR)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| AR-01/02 | default aspects | Python and Java in `languages`; `packages` present; under 60 s | 1 |
| AR-03 | `all` | under 120 s | 2 |
| AR-04/05 | `hotspots` names hub; `entry_points` include `main` | | 2 |
| AR-06 | `file_tree` with `path app` | `service.py` in, `Money.java` out | 1 |
| AR-07/08/09/11 | structure/dependencies/layers/boundaries; clusters/cycles; system_structure (polled twice, with behavior evidence) | run | 2 |
| AR-12/13 | invalid aspect; `routes` on a project with none | error; runs | 1 |
| AR-14..17 | `manage_adr` sections/get/set_sections/get | the written section is returned verbatim | 2 |
| AR-18 | `ingest_traces` of `caller_01→hub` | runs (argument names to confirm against the tool schema at the pilot) | 2 |
| AR-19 | Neo4j: `graph_backend.kind neo4j` (architecture itself is projected locally; the reply says what came from where) | | 1 |
