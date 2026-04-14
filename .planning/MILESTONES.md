# Milestones

## v1.0 GDScript Support Verification (Shipped: 2026-04-14)

**Phases completed:** 8 phases, 14 plans, 29 tasks

**Key accomplishments:**

- Pinned manifest approval metadata with canonical identity and Godot 4.x qualification rules for the Phase 1 proof corpus
- Proof harness output now exposes canonical approval identity, qualifying status, and regression coverage for manifest versus ad hoc runs
- Proof runs now emit a wrapper-first `run-index.json` that locates every repo artifact and preserves incomplete query evidence.
- Regression-backed proof documentation now describes `run-index.json`, wrapper-first evidence, and incomplete query inspection from repo-owned artifacts.
- Deterministic native pipeline mode selection with explicit forced-mode logging and parity regressions for later proof-harness dual-run validation
- Manifest proof runs now emit sequential and parallel wrapper-backed evidence for every approved repo plus additive semantic parity summaries for SEM-01 through SEM-06.
- Proof-harness semantic parity regressions now run inside `make test`, and the Phase 03 runbook/checklist explicitly require four-repo sequential-versus-parallel review with counts and representative samples.
- Forced-parallel incremental coverage plus a parser-before-slab worker cleanup fix for the Tree-sitter ASan crash path
- Manifest proof summaries now declare explicit repo verdicts and lead aggregate output with promotion-safe guidance bounded to the approved corpus and current commit.
- Runbook, checklist, and misses guidance now enforce verdict-first review with promotion-safe wording tied to the approved manifest corpus on the current commit.
- Persisted `--ui=true` / `--ui=false` launch coverage for the embedded localhost UI with a single runtime UI config path in `src/main.c`.
- Forced-parallel FastAPI Depends CALLS edges now survive both full and incremental native pipeline paths.
- Approved Nyquist validation contracts now exist for Phases 01-04, and the milestone audit reports complete validation-doc coverage for phases 01-05.
- GDScript `@export` variables now persist explicit exported metadata and anchor-linked variable qualified names through extraction and pipeline indexing.

**Known tech debt at ship time:**

- Phase 07.1 traceability gap: exported-variable support is verified but not mapped to an explicit REQ-ID in the milestone requirement table.
- Dependency hygiene warning: UI-related test path surfaced npm audit warnings that should be triaged in the next milestone.

---
