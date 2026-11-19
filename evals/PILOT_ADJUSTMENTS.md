# Pilot adjustments (first run, 2026-11-12)

Case-file changes made during the first execution, where the eval (not the product) was wrong.

| Step | Was | Now | Why |
|---|---|---|---|
| S01-20 | fast index has the same File count as full | 12 <= File count < full | `mode: fast` excludes `docs/` and test directories by design; the index reply lists them under `excluded` (19 vs 24 File nodes) |
| S02-10 | see case file | | SQLite picks the UNIQUE(source_id, target_id, type, ...) autoindex as a covering index; any source_id index is correct |
| S02-16 | see case file | | file_hashes column is rel_path, not path |
| S02-17 | see case file | | file_hashes column is rel_path, not path |
| SG-01 | see case file | | a Folder node is also named hub; restrict to label Function |
| SG-02 | see case file | | a Folder node is also named hub; restrict to label Function |
| SG-19 | see case file | | include_connected puts neighbours in a 'connected' column of the matched row, not as extra rows |
| SG-32 | see case file | | compose services are named Service/<name> (qn suffix docker-compose::compose/Service/-/<name>) |
| SG-07a | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07b | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07c | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07d | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07e | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| PG-11 | see case file | | default max_output_tokens (3200) truncates the 137-node page; raise the budget to test paging alone |
| PG-04 | see case file | | out-of-schema value is clamped, not rejected; kept as a low-severity finding at tier 2 |
| PG-06 | see case file | | out-of-schema value is clamped, not rejected; kept as a low-severity finding at tier 2 |
| QG-17 | see case file | | toLower() on the left of a WHERE predicate is a parse error; kept as a finding at tier 2; QG-17a checks the supported form |
| QG-17a | see case file | | supported form of QG-17 |
| QG-18 | see case file | | aggregate column is named COUNT(c) and values are strings |
| TP-19 | see case file | | compose services are found as Service/<name> |
| TP-20 | see case file | | out-of-range depth is accepted, not rejected; kept as a low-severity finding at tier 2 |
| TP-21 | see case file | | out-of-range depth is accepted, not rejected; kept as a low-severity finding at tier 2 |
| CS-27 | see case file | | an unknown file gives an empty outline (total 0), an honest answer |
| COV-16 | see case file | | aggregate column is COUNT(c), values are strings |
| COV-17 | see case file | | string concatenation with + is not in the Cypher subset; return the pair as two columns |
| COV-20 | see case file | | compose resources are named Service/<name> |
| COV-31 | see case file | | check the exact verdict fields |
| COV-32 | see case file | | check the exact verdict fields |
| COV-33 | see case file | | check the exact verdict fields |
| COV-37 | see case file | | check the exact verdict fields |
| COV-39 | see case file | | check the exact verdict field |
| COV-42 | see case file | | full diagnostics report parse_partial / parse_unusable / skipped / not_indexed, not a 'coverage' key |
| DC-04 | see case file | | format_cents lives in the changed file (a seed); its callers notify and Order.total are impacted |
| DC-05v | see case file | | via moved before the re-index so there are impacted rows |
| DC-08 | see case file | | after re-index the only seed is the new, uncalled extra_fn, so nothing is impacted; via is checked in DC-05v |
| DC-10 | see case file | | edge_types/fixpoint/via require direction inbound (documented error) |
| DC-18 | see case file | | compare_graphs requires two distinct projects; compare with a second project of the same repo |
| AR-16 | see case file | | set_sections was removed as a write primitive; write the ADR with mode update |
| AR-18 | see case file | | argument is traces: [{caller, callee}] with qualified names |
| S03-01 | see case file | | BM25 rows carry qn (no name column) |
| S03-02 | see case file | | BM25 rows carry qn (no name column) |
| S03-03 | see case file | | BM25 rows carry qn (no name column) |
| S03-08 | see case file | | BM25 rows carry qn (no name column) |
| gen_medium.py | see case file | | the indexer skips a directory named gen as generated code; the package root is now com/scale |
| N01-13 | see case file | | a Folder node is also named hub; restrict to label Function |
| N02-01..03 | see case file | | the cbm_project_name constraint has a backing RANGE index named cbm_project_name; count only cbm_node_* indexes |
| N02-12 | see case file | | PartialTop is the good outcome; forbid only a plain Sort/Top (exact operator names) |
| N02-29 | see case file | | a re-index with no source change skips the sync, so ensure_schema does not run; kept as a finding (tier 2) |
| N02-30 | see case file | | kept as a finding: the dropped index is still missing after a no-change re-index |
| N02-30a | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30b | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30c | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30d | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30e | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N03-10 | see case file | | kept as a finding: a docstring edit re-creates the file's rows (+4 -4) instead of changing them in place (~k) |
| N03-30a | see case file | | a sync is skipped when the generation is unchanged; deleting the state file alone does not force one |
| N04-02a | see case file | | a sync is skipped when the generation is unchanged |
| N04-17 | see case file | | DEFECT PIN kept: after the graph was deleted inside Neo4j, a no-change re-index answers 'unchanged' and serves an empty graph |
| N04-19a | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19b | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19c | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19d | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19e | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19f | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N06-06 | see case file | | kept as a finding: native replies carry no graph_backend block (tier 2) |
| N07-06a | see case file | | pin the credential-poisoning defect and isolate later steps from it |
| N07-06b | see case file | | pin the credential-poisoning defect and isolate later steps from it |
| N07-17 | see case file | | count only cbm_node_* indexes (the constraint has its own backing index) |
| N05-24 | see case file | | DEFECT PIN: the existing project must keep reading after another project's failed login |
| N05-25/26 | see case file | | restart the daemon after the bad-login pin |
| N05-12 | see case file | | kept as a finding: check_index_coverage is refused while Neo4j is down although its data is local (tier 2) |
| N99-06 | see case file | | count only cbm_node_* indexes |
| N09-19 | see case file | | BM25 rows carry qn (no name column) |
| DC-17 | see case file | | on a clean tree detect_changes reads no graph rows, so it names no backend; accept that |
| N08-02/03/04/12, compare_backends.py | see files | | normalise repo paths and both project names; ignore BM25/semantic score values and row order where the twin projects' different names change them; skip environment-dependent steps (project lists, ADR state, a different second project) |
| SG-26/27 | see case file | | raise max_output_tokens so the comparison is not cut by the output budget (the longer project name fits fewer rows) |
| N02-07..15 | see case file | | EXPLAIN on a tiny graph is cost-dependent (planner chose NodeIndexScan); demoted to tier 2; PROFILE with a database-access ceiling on eval-medium is the tier-1 check |
| N09-10 | see case file | | the next class in the same package: new C().run() resolves to run() |
| N09-10/10b/10c | see case file | | the cross-package chained call resolves to the class, not run(); tier-1 check moved to a same-package pair, cross-package kept as a tier-2 resolver finding |
| N09-12..15 | see case file | | raise max_output_tokens: the default budget truncates 500-row and 5,000-row pages |
| N09-30..36 | see case file | | new: PROFILE the product's Neo4j query shapes on the 120k-node graph with database-access ceilings |
| N04-07 | see case file | | a Folder node is also named hub; restrict to label Function |
| N06-19 | see case file | | the engine treats -- as a comment, so the hidden pattern is ignored; safe outcome is an error or this project's own count |
| N06-27 | see case file | | a Folder node is also named hub; restrict to label Function |
| N08-07/08 | see case file | | compare content unordered; the tie order of trace callers and snippet suggestions differs between backends and is reported as a finding |
| N09-35 | 60,000 | 80,000 | the project filter adds one property read per row: 60,010 accesses for a 20,000-row page is the intended ~3 per row; the ceiling was 10 accesses too tight |
| S99-04 (assertions.py) | | | an empty table has no column names; 'excludes' now holds on an empty list |
| expected/eval-small.json | orders, db | Service/orders, Service/db | compose services are indexed as Service/<name>; the two tier-2 'misses' in the first coverage report were this naming slip |
| N09-22, run_cases.py | | | fs steps may name their own root (the medium repo); an unexpected exception is now recorded as a failed step instead of ending the suite (N09-22..27 had not run) |

# Round 2 (2026-11-18, DEV 87bc455b + branch fix/eval-findings)

Re-run of every suite on both backends (with `--with-scale`). Baseline on DEV:
836 pass, 50 fail (19 tier-1). After the product fixes on `fix/eval-findings`
and the eval corrections below: 887 pass, 1 fail (tier 2), 0 tier-1 failures. The one
tier-2 failure is N08-03 (SEM-01): semantic vectors include the project name, so the
two twin projects rank a few rows differently; it is expected.

## Eval corrections (the eval was wrong)

| Step | Was | Now | Why |
|---|---|---|---|
| PG-04, PG-06 | limit 0 / offset -1 rejected | clamped (limit 1, offset 0) | search_graph clamps out-of-range paging by design (#887); it never returns a wrong page |
| TP-20, TP-21 | depth 16 / 0 rejected | clamped to 15 / 1, 12 hub callers | trace_path clamps depth like the other tools; depth 0 used to answer "no callers" (fixed in the product) |
| N05-12 | check_index_coverage works while Neo4j is down | refused, names Neo4j | coverage reads graph nodes; graph reads never fall back to the local index |
| N06-25b, N06-26b | (new) | index / delete the SQLite twin inside N06 | the SQLite profile's own cleanup deletes the twin, so N06-26 hit "project not found" |
| SEM-02 | phrases | keywords | semantic_query takes keywords; each is scored on its own |
| SEM-05, SEM-06 | semantic rows filtered by label / file | structural page filtered, semantic set returned beside it | documented contract: combined calls keep both independently ranked result sets |
| SEM-08 | SEMANTICALLY_RELATED present | schema lists CALLS | eval-small has no bodies similar enough; BATCH-SRVC has thousands of such edges |
| N03-10 | docstring edit: +0 -0 nodes | small delta, single-digit adds/removes | the incremental route re-creates the edited file's nodes with fresh ids (contentless FTS rows are never reused) |
| SG-07d | bracket class in file_pattern | brackets are literal | by design: `pages/[id].tsx` must stay findable; classes belong in name/qn regexes |
| SG-07a/b/c, N07-06a | DEFECT PIN | REGRESSION (fixed) | the defects they pinned are fixed on fix/eval-findings |

## Product defects found and fixed (branch fix/eval-findings)

| Steps | Defect |
|---|---|
| SG-07, SG-07a, SG-07c | regex LIKE pre-filter treated `[a-z]`, groups and optional atoms as literal text: 0 rows |
| SG-07b | POSIX ERE has no `\d`: `fn\d+` meant "fn" + letter d; cbm_regcomp now rewrites `\d \w \s` (and negations) |
| N04-09..19 | a graph deleted in Neo4j (or its pointer moved) was served as an empty answer and an unchanged re-index skipped the upload |
| N05-24, N07-06a | a wrong graph_password on one index request replaced the daemon's remembered login for every project on that server |
| N02-30 | an unchanged re-index skipped the schema step, so a dropped Neo4j index stayed dropped |
| N06-06 | native query replies did not say they were served by Neo4j |
| QG-17 | toLower / toUpper / toString in WHERE were a syntax error |
| QG-27 | syntax errors said "expected token type 85, got 68" |
| S03-08 | BM25 dropped non-ASCII bytes: an accented identifier was unsearchable |
| TP-21 | trace_path depth 0 answered "no callers, no callees" |
| N08-02 (SG-25) | invalid regex: different error text per backend |
| N08-11 | get_architecture tie order differed between two indexes of the same code |
| N09-10b/c | Java: calls on a type imported from another package went unresolved (19,800 of 19,999); on BATCH-SRVC the type-aware share of CALLS rose from 17% to 36% and about 28k call sites now point at the right method |
| (perf, BATCH-SRVC) | Neo4j: a regex name search fetched every candidate of a weak literal hint (now pre-filtered with =~ in Neo4j); relationship filter read every relationship's type property (now a typed EXISTS, 4.6 s -> 0.5 s); query_graph scans of 50k-350k nodes streamed through the engine for ~5 s instead of Neo4j answering in < 0.5 s |
