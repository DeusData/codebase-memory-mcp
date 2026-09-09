# PR 2068: repository map implementation record

## Starting state (2026-09-09)

The supplied workspace was empty. Cloned `DeusData/codebase-memory-mcp`,
`feat/codeatlas-web`, at `80f4f41f9bcb9daf60afc5f607bcbcba24a4b196`.
The checkout was clean. PR base is `feat/atlas-r1`. No other branch, user
worktree, remote, or Git history has been modified.

Read PR description, all five discussion comments and changed-file list;
reviewed the architecture/front-end and daemon seams in the local diff/source.
The latest branch also contains the exploration, Architecture, Activity and
System workspaces, beyond the original PR description. `CONTRIBUTING.md`
supplies build/test/security conventions. There is no checked-in AGENTS.md.
The user's explicit instruction authorizes implementation on this existing PR.

The C daemon coordinates MCP clients and owns HTTP on one listener. Graph
databases are per-project SQLite. Agent hooks currently write JSONL; a Node
bridge on 4142 forwards SSE. UI logs POST to the daemon but persist separately
as a rotated file; `/api/logs` reads a bounded process-memory ring. Architecture
uses `get_architecture` and indexed files/callable sweeps. Group boundaries
carry counts but no drill-through to their constituent edges. The older impact
modal follows changed-file callers but does not use Git co-change evidence.
“Why are you here?” is an onboarding task chooser, not contextual relevance.
Coverage already joins `index_status` and paginated `check_index_coverage`;
this is the diagnostic source to preserve.

## Verification setup

Production embedded baseline built with `scripts/build.sh --with-ui
BUILD_DIR=build/baseline`. Actual repository indexed via `POST /api/index`,
project `cbm-pr2068`, isolated cache `/tmp/cbm-2068-cache` and rendezvous
`/tmp/cbm-2068-runtime`. One daemon HTTP listener at 127.0.0.1:9749. User
configuration and existing indexes remain untouched. Browser artifacts live
under `graph-ui/verification/pr-2068`; generated screenshots are actual browser
captures, not illustrations.

Baseline frontend: **157 files / 2334 tests passed**, `npm run test:unit`.

## Implementation order

1. Durable local agent/log store with bounded same-port polling and replay.
2. Bounded structural impact and Git history, evidence and uncertainty apart.
3. Repository → subsystem candidate → file → symbol and relationship evidence.
4. Selection context from static graph and separately observed tool events.
5. Shared visible failure feed and explicit local diagnostic review.
6. End-to-end browser checks, sanitizer/unit/contract gates and final limitations.

No new frontend framework or model dependency is planned. Research findings,
identity ambiguity and licensing decisions are recorded separately.

## Delivered implementation

The implemented behavior, final tests, real before/after screenshots, research
decisions and limitations are recorded in [pr-2068-delivery.md](pr-2068-delivery.md).
The full native run passed 7,954 tests across 141 suites, with seven skips.
A subsequent genuine unreadable-file E2E exposed a clean-worker / failed-tool
status mismatch; the coordinator fix passed 129 focused sanitizer tests.
Final frontend: 166 files / 2,377 unit tests, 203 acceptance contracts, TypeScript,
production build, style and promise gates passed. Global C lint remains open
for the documented baseline/style/tooling reasons. No push was performed.
