# index-artifact-export-error-surfacing Specification

## Purpose
Defines how `index_repository` reports a post-publish persistence artifact export failure: the run result stays failed (the in-tree policy is deliberate), but the error response names the actual cause — the artifact export into the repo — instead of blaming `repo_path`.

## Requirements

### Requirement: Failure attribution names the artifact export
When `index_repository` returns a non-zero result and the failure was the post-publish artifact export (`export_after_publish`), the response SHALL carry a hint that states the persistence artifact export failed, names `.codebase-memory/` (and the export error detail when available), and states that the index database itself was published — the remedy being a writable checkout or running without `--persistence`.

#### Scenario: persistence export fails on a read-only repo
- **WHEN** `index_repository` runs with `--persistence true` on a read-only repository and the graph database publishes successfully while the artifact export fails
- **THEN** the returned result has `status: "error"` and its hint names the artifact export failure (with the artifact directory and the export error), does not claim the repository path is missing or source-less, and does not recommend a mode change as the remedy

#### Scenario: a genuine pipeline failure keeps the existing hint
- **WHEN** `index_repository` fails before publish (e.g. the repository path does not exist or is empty)
- **THEN** the hint is the existing generic message ("Pipeline failed. Check repo_path exists and contains source files"), unchanged, and no artifact-export wording is shown

### Requirement: The surfaced error is from the current run only
The pipeline SHALL capture the artifact export error of the run it just executed and expose it to the response builder; the response SHALL NOT be built from obsolete global error state belonging to a previous run.

#### Scenario: previous run failed, current run fails earlier
- **WHEN** a run fails after publish due to artifact export, and a later run fails before export (or never requests one)
- **THEN** the later response does not mention the earlier run's artifact export error in its hint
