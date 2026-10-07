# PR 2068: review call feedback, reproduction, plan and acceptance

Prepared on 2 October 2026 (Europe/Berlin) after the review call between Martin and Bernhard. Base: `feat/codeatlas-web` at `50a6a67b89f2e2bd901b903fe0f2affabc1cb449`. The plan below was approved as proposed (all ten decisions) and is implemented on `fix/atlas-call-feedback`; the measured acceptance is in [Implementation and acceptance](#implementation-and-acceptance) at the end.

Every point from the call was reproduced in a real browser before a fix was proposed. Where the call notes or the first code reading guessed a cause, the measurement below decides. Two guesses were refuted that way; both are marked.

## How the evidence was produced

- **Binary:** built from `50a6a67b` with the UI embedded (`scripts/build.sh --with-ui` into a separate `BUILD_DIR`). It ran with an isolated `HOME`, `CBM_CACHE_DIR` and `CBM_RUNTIME_DIR` on a free port, so no existing installation or daemon was touched.
- **Indexes:**
  - Django 5.2.7, a shallow clone indexed as project `django-demo`: 52,402 nodes and 274,549 edges, indexed in 19.5 s.
  - This repository at `50a6a67b` as a control, project `cbm`: 44,214 nodes and 178,535 edges.
- **Browser:** Chromium from Playwright 1.62.1, headed with the GPU, because the browser model needs WebGPU. Viewport 1600x1000 CSS pixels at device scale 2.
- **Capture script:** [`graph-ui/tools/call-feedback-capture.mjs`](../../graph-ui/tools/call-feedback-capture.mjs). Every interaction becomes a numbered series of screenshots: before the action, on hover, at the click, then after 250 ms, 1 s and 3 s, plus every wheel step on its own. A ring marks the mouse position, because screenshots do not show the cursor. For each image the script also records:
  - the identity of the canvas element (a remount gets a new number)
  - the camera, via `__atlasGalaxyFit.measure()`
  - the `__atlasGalaxy` counters
  - the messages sent to the browser model, which contain the prompt it actually received
- **What is committed:** the full series (121 images, 86 MB) stays local under `graph-ui/verification/call-feedback-2026-10-02/` and is not committed. The 17 images in [`pr-2068-call-feedback/`](pr-2068-call-feedback/) are downscaled JPEGs, 2.3 MB in total.

## Corrections to the call notes

The notes were written by a speech-to-text summary without the repository. These items read differently against the code and the screenshots:

| Notes | Actual |
| --- | --- |
| "GraphiQL" as visual reference | Graphify (`Graphify-Labs/graphify`), its shortest-path view: path nodes highlighted, edges labelled ("uses", "references"), the rest dimmed |
| "JSON-Bag", "jsonl.agg" | `JSONBAgg` in `django/contrib/postgres/aggregates/general.py` and its tests `test_jsonb_agg_*` |
| "Trace Path, an LCP for it" | The MCP tool `trace_path`. Galaxy does not use it; Galaxy reads `query_graph` only |
| "Interactive 2.5 Coder 0.5" | Qwen2.5 Coder 0.5B, running in the browser on WebGPU (transformers.js), not llama-server |
| Edge types "Calls, Test, Defines, Inherits" | INHERITS, CALLS, IMPORTS, TESTS, DEFINES_METHOD, USAGE, DECORATES, DEFINES |
| "Output stops at item 16, output token limit suspected" | No hard limit of 16 exists. Manual chat gets 512 output tokens and decodes greedily without a repetition penalty. In the reproduction the answer repeats one line from item 13 and stops at item 48 |
| "Django: System structure and Behavior empty because of a budget" | Not the node or edge budget: Django is below both. It is the 32 MB JSON budget of the projection (see S1) |

## Findings and proposed changes

Priorities:
- **P1:** release-relevant bugs.
- **P2:** improvements that need a decision (see the list at the end).
- **Backend:** C changes, proposed only.

### Galaxy

**G1 (P1) The view resets: the canvas is rebuilt on every scope step.**
- **Measured:** the canvas element changes on every step. Selecting `JSONBAgg` goes from element 1 to no canvas, then element 2. Each `Expand +1` replaces it twice (3, 4, then 5, 6). The fit counter rises with every step (1, 2, 4, 6), and the camera jumps to a new orientation each time ([blank canvas right after the click](pr-2068-call-feedback/01-galaxy-select-blank-canvas.jpg)).
- **Side observation:** in 2 of 4 runs, `Cannot read properties of null (reading 'addEventListener')` was thrown during a selection. It is not localized yet and is probably a listener attached during the remount.
- **Cause:** `useOrganicLayout` returns no result while it computes (`galaxy/use-organic-layout.ts:75`). `shown` becomes `undefined` and `GraphScene` unmounts, since it is mounted only while `shown` exists (`galaxy/GalaxyPanel.tsx:2385`). Each new picture also counts as a new fit request (`GalaxyPanel.tsx:1435-1503`, `fitRequest` at 1442). A stale fly-to replays on mount (`galaxy/GraphScene.tsx`, `CameraAnimator` 165-320).
- **Change:**
  - Keep the last laid-out picture visible, marked stale, while the next layout runs, so the scene stays mounted.
  - Refit only when the root, direction or edge types change, not on expansion or on the step from preview to complete result.
  - Do not replay an old fly-to on mount.
- **Tests:** `GalaxyPanel.test.tsx` gets remount and fit counters through `__atlasGalaxy`. `use-organic-layout.test.tsx` keeps "never presents the previous source as current" by exposing the previous picture as stale, not as current.

**G2 (P1) A symbol opens as an isolated node with no edges.**
- **Measured:** after selecting a symbol the toolbar reads "0 layers · 1 node · 0 edges" ([image](pr-2068-call-feedback/02-galaxy-select-isolated-node.jpg)). The edge-type filter then says "No edges in this view" and cannot be used until the user expands ([image](pr-2068-call-feedback/03-galaxy-edge-filter-empty-at-depth-0.jpg)).
- **Cause:** `select` sets depth 0 (`galaxy/use-graph-scope.ts:130`). `loadGraphScope` runs zero hops for a single root (`galaxy/graph-scope.ts:136`).
- **Change:** node and symbol scopes start at depth 1, and the minus button stops at 1 for them. File and folder scopes keep depth 0; they already include their internal edges.
- **Tests:** `use-graph-scope.test.tsx` load-count expectations.

**G3 (P1) Zoom goes to the screen centre, never to the cursor.**
- **Measured:** wheel zoom moves the camera exactly along its view direction (ratio 1.000) for both cursor positions. That holds for the single node, for one layer around `JSONBAgg` (49.6 units) and for the whole graph (1,058 units). The Architecture scenes behave the same way.
- **Cause:** the OrbitControls have no `zoomToCursor` (`galaxy/GraphScene.tsx:933`, `architecture/ArchitectureScene.tsx:340`, `architecture/SystemArchitectureScene.tsx:286`). The installed three-stdlib 2.36.1 supports it and drei passes the prop through.
- **Change:** enable `zoomToCursor` in all three scenes, and check that it interacts correctly with the custom camera animator.
- **Verification:** the same capture. With the fix, the ratio must drop clearly below 1 and the node under the cursor must stay under it.

**G4 (P1) The starting node is lost after expanding.**
- **Measured:** at one layer `JSONBAgg` is still the visual centre. At two layers (90 nodes, 201 edges) it cannot be found ([image](pr-2068-call-feedback/04-galaxy-two-layers-root-lost.jpg)). `Func` at one layer (99 nodes, 112 edges) is the starburst from the call, with labels stacked in the centre ([image](pr-2068-call-feedback/05-galaxy-func-starburst.jpg)).
- **Cause:**
  - The organic layout pins the root only through `previous`, which is dropped when `organicKey` changes (`galaxy/organic-clusters.ts:285-305`).
  - The fit centres the cloud, not the root (`galaxy/camera-frame.ts:fitCamera`).
  - Screen separation pushes nodes radially and may move them out of view (`galaxy/screen-node-spacing.ts:78-112`).
  - Nothing marks the root (`GalaxyPanel.tsx:2393`).
- **Change:**
  - Pin the roots at the origin across expansions.
  - Give the root its own marker (ring and halo) and an always-visible label.
  - Centre the fit on the root.
  - Exclude the root from separation, and fit after separation so no node ends up outside the view.
  - The organic layout stays: rings were rejected deliberately (`organic-clusters.test.ts:109`).
- **Tests:** `organic-clusters.test.ts` (root fixed across expansion), `camera-frame.test.ts`.

**G5 (P2) Tracing and path view.** Step-by-step call order is missing, as said in the call. Proposal: a path overlay modelled on Graphify.
- Pick a target node; the shortest path inside the loaded scope is highlighted with labelled edge types and everything else is dimmed.
- Step through it hop by hop, and order calls by `edge.line`.
- Data: the scope edges and `levels` that are already loaded, so both directions work without a backend change. `/api/trace` today is outbound only, CALLS or data, and callables only (`src/ui/atlas_flows.c:770-867`).
- Two small bugs to fix on the way:
  - `RpcIntelligenceClient.tracePath` sends `callers`/`callees` and `max_depth`, but the server expects `inbound`/`outbound` and `depth` (`graph-ui/src/provider/rpc-client.ts:335-346`).
  - The hop columns of `scopedHierarchy` are wrong while loading, because the hop is derived from global z values (`galaxy/graph-scope.ts:215`).

**G6 (P2) Distance with meaning** (hops, code distance, or file and folder hierarchy). Proposal: use hops only, and only inside the path view and the hierarchy view. The overview stays as it is.

**G7 (P1, small) The toolbar wraps.** At 1600 CSS pixels the Galaxy toolbar breaks into a second row ("Edges 20.000" moves down), even with the chat closed.

### Chat and local agent

**C1 (P1) The model reads and repeats internal field names.**
- **Measured:** the prompt actually sent for `JSONBAgg` is a dump of JSON paths:
  ```
  Snapshot.view: "galaxy"
  Snapshot.generation: "unavailable"
  Scope.renderedNodes: 15
  Relationships.items[0].source.qualifiedName: ...
  Upstream omissions[0].path: "$.relationships.items[17].source"
  ```
- **What the model makes of it:**
  - The automatic explanation invents names such as `djangodevelopers.postgres.aggregates.jsonbaGG` and stops mid-sentence ([image](pr-2068-call-feedback/07-chat-explanation-hallucinated-truncated.jpg)).
  - For `BaseCommand` it says the command "has no roots", its "scope is not complete and requires indexing" and there are "up to 4 upstream omissions". At the same time the graph shows 130 nodes ([image](pr-2068-call-feedback/09-chat-basecommand-internal-keys.jpg)).
- **Cause:** `GalaxyPanel.tsx:1137-1157` builds the evidence, and `browser-ai/explanation-context.ts:115-187` turns it into `path: value` lines.
- **Change:** format Galaxy evidence as readable facts. For example: the selection with kind and file; incoming relationships grouped by edge type with complete counts and names; then outgoing ones. Internal keys (snapshot, view, generation, renderedNodes, omission paths) never reach the prompt.

**C2 (P1) "Who calls JSONBAgg?" gets no caller at all.**
- **Measured:**
  - **Index:** 23 incoming rows: 11 test functions in `tests/postgres_tests/test_aggregates.py` with CALLS, the same 11 with TESTS, and DEFINES from `general.py`.
  - **Prompt:** contains exactly one of 18 relationship items, the outgoing INHERITS of `JSONBAgg` itself, followed by "14 additional graph summary lines omitted". Not one caller name.
  - **Answer:** it lists `django.db.models.fields.*`, repeats `django.db.models.fields.DateTimeField` from item 13 to item 47 and stops at item 48 ([image](pr-2068-call-feedback/08-chat-who-calls-repetition.jpg)).
- **Cause:**
  - Arrays are cut to three items before the budget is even applied (`explanation-context.ts:120`).
  - Edges are cut to 24 (`GalaxyPanel.tsx:1149`).
  - Callers and callees are mixed under `both`.
  - Manual chat decodes greedily with no repetition penalty and 512 tokens (`browser-ai/browser-ai.worker.ts:132-152`, `browser-ai/model-policy.ts:45`).
- **Change:**
  - Caller and callee questions get a deterministic list from the loaded graph, complete counts by edge type, then names up to the budget, then "+N more". The model only phrases it, as the PR design already intends ("deterministic source-cited answers with an optional local-model wording pass").
  - Apply the repetition penalty and n-gram block in manual chat too.
  - The worker reports why it stopped, and the UI shows "shortened (token limit)".
  - Show a capacity note when the scope does not fit the prompt, e.g. "104 nodes / 117 edges: too large for the local 0.5B model; showing N".
  - Trim history to the input budget (`chat-model.ts:97-101`).

**C3 (P1) "Selection details" covers the chat input and blocks the send button.**
- **Measured:** the toggle sits at x 1452-1588, y 922-961, inside the input at x 1193-1588, y 904-965 ([image](pr-2068-call-feedback/06-chat-selection-details-covers-input.jpg)). Playwright cannot click Send: `<summary>Selection details</summary> ... intercepts pointer events`. The question had to be sent with Enter.
- **Cause:** `.galaxy-selection-evidence` is absolutely positioned. `.atlas-galaxy` has no `position` in the Galaxy workspace, so the box is placed relative to `.atlas-workspace-content`, the grid that also holds the chat column (`why/selection-context.css:13`, `galaxy/graph-exploration.css:33`, `styles/workspace.css:18-36`).
- **Change:** `position: relative` for `.atlas-galaxy` in the Galaxy workspace. Verify with chat open, chat closed and fullscreen.

**C4 (P1) Explanations are generated again on every return.**
- **Measured:** going back to the same selection shows "Explaining selection..." and the agent runs again.
- **Cause:** there is one explanation slot and no cache per selection (`BrowserChatDock.tsx:77-169`). The key contains values that change while a scope loads (`proactive-selection.ts:22`). An explanation can also start before the expansion completes; the `BaseCommand` text above describes the state before it.
- **Change:**
  - A per-selection cache keyed by project, workspace, root id, direction, edge types and depth.
  - No volatile counts in the key.
  - Start an explanation only once the scope is complete.

**C5 (P2) Token limits in the agent configuration.**
- **Measured:** the dialog offers model, automatic explanations and history only ([image](pr-2068-call-feedback/10-agent-configuration-no-token-limits.jpg)). Model choice and the automatic flag are `useState` only and reset on reload (`BrowserChatDock.tsx:76, 89`).
- **Change:** input and output token limits per model, bounded by `model-policy.ts`, stored in the browser following `settings/view-preferences.ts`. Also store the model choice and the automatic flag.

**C6 (P2) Header.**
- **Current:** "● Agent active · Qwen2.5 Coder 0.5B" next to the panel toggle ([image](pr-2068-call-feedback/11-header-agent-status.jpg)).
- **Proposal from the call:** a robot icon with a status lamp; a click opens the agent configuration (it already does); the model name moves to the tooltip and the configuration.
- **Code:** `app/AtlasChrome.tsx:680-689`. `AtlasChrome.test.tsx:94` expects the model name in the header and changes with it.

### Architecture

**A1 (P1) Overview loses its perspective after opening a large area.**
- **Measured:**
  - The Django root shows 7 parts in the tilted 3D view ([image](pr-2068-call-feedback/12-overview-django-root.jpg)).
  - After "Open area" on `django`, the plates fill the whole canvas. Full-path labels ("django/contrib/...") overlap, and the footer reads "Partial map · 40 parts" ([image](pr-2068-call-feedback/13-overview-django-drill-in-flat.jpg)).
  - "Fit map" and three wheel steps do not change that.
  - Control: opening `src/mcp` in this repository keeps the perspective, but external areas (`src/cli`, `graph-ui`, `(root)`) sit inside the picture ([image](pr-2068-call-feedback/14-overview-cbm-drill-in-control.jpg)).
- **Cause:**
  - The folder layout is computed for every file of the area, but only 40 are drawn. The pruned platforms keep their full size and the fit frames them (`architecture/semantic-graph.ts:425, 446`).
  - Areas are one folder deep except under `src|lib|internal|packages|apps|services`, so `django/` jumps straight to files (`architecture/repository-map.ts:25-34`).
  - File heights are scaled against the largest area (`architecture/source-metrics.ts:120-124`).
  - Labels are full paths and are never culled outside hotspots (`ArchitectureScene.tsx:338`).
- **Change:**
  - Opening an area shows its next folder level as area blocks, with `areaOf` relative to the current path.
  - Cap before the layout.
  - Scale heights per scope.
  - Use basename labels with `adaptiveLabels`.
  - Put external areas in a separate lane.
  - Goal: the same perspective at every level.

**A2 (P1) System structure is empty for Django and does not say why.**
- **Measured:** `get_architecture` (`system_structure`) returns `status: "limited"` with the warning "The architecture response exceeded its memory budget; narrow the requested projection." after 451 ms, with 0 components and 0 entry points. Totals are 52,402 nodes and 274,549 edges, below the 100,000 / 500,000 budgets.
- **Refuted guess:** the edge budget does not explain this. The limit is the 32 MB JSON budget (`src/store/architecture_projection.c:18`, `2008-2012`).
- **What the UI shows:** "No component projection is available within this analysis budget." with the default filters ([image](pr-2068-call-feedback/15-system-structure-django-empty.jpg)), and "No component cycles match the current filters" with "Group cycles" enabled (the call screenshot). The actual warning is visible only inside the collapsed "Evidence and limits" section.
- **Frontend change:** when the result is limited or has zero groups, show the warning text in the empty state, and choose the message before the cycle filter (`architecture/SystemArchitecture.tsx:286-299, 371-382`).
- **Backend (proposal):** a repository of Django's size should not exceed the response budget with the displayed projection already capped. Martin to decide whether to cap earlier or stream.

**A3 (P1) Behavior is empty for Django.**
- **Measured:** the start list holds only "Choose an operation…" for Django ([image](pr-2068-call-feedback/16-behavior-django-empty.jpg)). For the control repository it holds 128 operations (`main` plus TS/JS exports).
- **Causes:**
  - The projection is limited, see A2, so no entry points are emitted.
  - Python entry points are only functions named `main` (`internal/cbm/extract_defs.c:3815-3826`).
  - The frontend auto-pick accepts only `main` (`architecture/SystemArchitecture.tsx:25-31`).
  - `behavior.source_id` arrives as `0` when nothing is chosen, and `??` does not fall through on `0` (`architecture/behavior-journey-model.ts:104`).
- **Frontend change:**
  - Treat `0` as no selection.
  - When there are no entry points, fill the list from `/api/flows` (route handlers, call-graph roots), or allow searching any Function or Method.
  - Widen the auto-pick beyond `main`.
- **Backend (proposal):** Python entry points beyond `main`, such as Django views and route handlers. `ap_test_path` also marks `django/test/` as test code and misses `tests.py`.

**A4 (P2) Routes, Endpoints: the idea is good but the view is verbose.** Dozens of route labels overlap, most of them registered under `tests/` ([image](pr-2068-call-feedback/17-routes-endpoints-django.jpg)). Proposal:
- group by first path segment or app folder, with counts
- hide test routes by default
- enable `adaptiveLabels`

The model already supports `filter` and `maxNodes`, but the UI hard-wires the filter to `''` (`architecture/ArchitecturePanel.tsx:115`).

### Confirmed as intended, no change

- Hotspots of folders are drawn flat on purpose.
- Explorer and Galaxy share the same selected-node view on purpose.
- System shows configuration, indexes, watchers and "Errors and logs" with a severity filter. How an error entry stands out was not exercised, since no error occurred during the run.

### Side finding: the style gate fails on `50a6a67b`

`node tools/style-gate.mjs` in `graph-ui/` exits 1 on the unchanged PR head. The new files of this branch add no hit.

- 16 long dashes, in `fixtures/adr/*.md`, `src/browser-ai/BrowserChatDock.test.tsx` and `src/browser-ai/reader-chat-context*.ts`.
- 11 hardcoded chrome strings: `src/app/AtlasChrome.tsx` (7), `src/App.tsx` (3), `src/projects/ProjectsPanel.tsx` (1).
- 2 attribution name hits outside the rule files, in `README.md` and `src/settings/ConfigReference.tsx`.

`scripts/ci/test-ui.sh` runs this gate, so the `test-ui` job would fail on the current head. I propose fixing it together with the P1 commits, unless Martin prefers to do it himself.

## Decisions requested

1. Depth 1 as the start for node and symbol scopes, with a minimum of 1 (G2).
2. Pin and mark the root and centre the fit on it, while keeping the organic layout (G4).
3. Path view as an overlay on the loaded scope; `/api/trace` stays unchanged for now (G5).
4. Distance as hops, only in the path and hierarchy views (G6).
5. Deterministic caller and callee lists in chat, with the model for wording only (C2).
6. Token limits and model choice stored in the browser, per model, within the model policy (C5).
7. Robot icon with a status lamp in the header; the model name moves to the tooltip (C6).
8. Routes grouped, with test routes hidden by default (A4).
9. Backend, for Martin: the JSON budget for Django-sized projections (A2), Python entry points and `ap_test_path` (A3).
10. Evidence policy: only these 17 images are committed; the full series stays local.

## Order and verification after approval

1. **P1 frontend fixes**, one commit per topic: G1, G2, G3, G4, C3, C1, C2, C4, A2, A3, A1, G7. Each comes with focused vitest coverage:
   - `GalaxyPanel.test.tsx`, `use-graph-scope.test.tsx`, `use-organic-layout.test.tsx`, `organic-clusters.test.ts`
   - explanation-context and chat dock tests
   - `behavior-journey-model.test.ts` with `source_id: 0`
   - `SystemArchitecture.test.tsx` with an empty limited result
   - `semantic-graph.test.ts` above 40 nodes
2. **P2 items** as approved.
3. **Final checks:** `scripts/ci/test-ui.sh`, then the same capture script against Django and the control repository. Before and after appear side by side, with the canvas, camera and prompt measurements above as acceptance criteria.

## Implementation and acceptance

All items G1 to G7, C1 to C6, A1 to A4, the backend parts of A2 and A3 and the style gate side finding are implemented on `fix/atlas-call-feedback`, 49 signed-off commits on top of the plan commit. The decisions were applied exactly as proposed.

### Browser acceptance

[`graph-ui/tools/call-feedback-acceptance.mjs`](../../graph-ui/tools/call-feedback-acceptance.mjs) checks every item in the running UI: a binary built from this branch with the UI embedded, the same Django 5.2.7 index and the control repository. Each check measures a value in the page instead of judging a picture. Movements are also cut into frame strips (six frames per second) from a recorded video. Final run: **19 of 19 checks passed, no page errors.**

| Item | Measured after the fixes (before) |
| --- | --- |
| G1 | One canvas element through selection and two expansions (before: rebuilt twice per Expand). Fits: one per new root, none per Expand. While the first scope arranges, the whole graph stays drawn instead of a blank canvas ([frame strip](pr-2068-call-feedback/30-after-strip-select-scope.jpg)) |
| G2 | A symbol opens at "1 layer" with 25 drawn edges for `JSONBAgg`; the minus button is disabled at depth 1 (before: "0 layers · 1 node · 0 edges") |
| G3 | Wheel zoom moves the camera 0.87 and 0.85 along the view direction, so it turns toward the cursor (before: exactly 1.000, always to the centre) |
| G4 | After two layers (90 nodes) the root marker sits at the canvas centre and all 90 nodes are inside the view ([image](pr-2068-call-feedback/21-after-galaxy-two-layers-root-marked.jpg)) |
| G5 | "Path to Func · 2 hops" via `Aggregate` with labelled edges, stepping and Escape ([image](pr-2068-call-feedback/22-after-galaxy-path-view.jpg)). Call order of `call_command`: 8 calls in source line order 110 to 173 ([image](pr-2068-call-feedback/23-after-galaxy-call-order.jpg)) |
| G7 | Scoped toolbar 49 px high at 1600 px, one row |
| C1 | The explanation prompt names all 11 callers from the index and contains none of the internal keys (before: one outgoing INHERITS item and `Snapshot.*`, `renderedNodes`, omission paths) |
| C2 | "Who calls JSONBAgg?" lists all 11 CALLS callers, the 11 TESTS edges and DEFINES from `general.py`, marked "Listed from the indexed graph; not generated by the model", with no repeated line ([image](pr-2068-call-feedback/24-after-chat-who-calls-listed.jpg)) (before: `DateTimeField` repeated to item 48) |
| C3 | The element under the Send button is the Send button; Selection details ends at x 1158, the input starts at x 1193 (before: Selection details intercepted pointer events) |
| C4 | Returning to `JSONBAgg` starts no new model run and shows the same explanation at once (before: "Explaining selection..." again) |
| C5 | Output limit set to 384, still 384 after a reload; input limit 2048 ([image](pr-2068-call-feedback/25-after-agent-configuration-token-limits.jpg)) |
| C6 | Robot icon with status lamp; the model name is in the tooltip and label; a click opens the agent configuration |
| A1 | Inside `django`: 23 labels, none overlapping, no full paths; the next folder level as area blocks, external areas in an "Outside" lane, the same tilted perspective as the root ([image](pr-2068-call-feedback/26-after-overview-django-drill-in.jpg)) |
| A2 | System structure for Django: 12 groups shown; backend status `ready` with 12,774 components in 458 ms (before: `limited`, 0 components, reason hidden) ([image](pr-2068-call-feedback/27-after-system-structure-django.jpg)) |
| A3 | Behavior offers 61 starts for Django: the 2 classified entry points first, then ranked flows; `main` is picked and shows its 2 direct callees ([image](pr-2068-call-feedback/28-after-behavior-django.jpg)). The control repository keeps 129 |
| A4 | Endpoints grouped by first segment with counts, 155 test routes hidden behind a toggle, 20 labels without overlap, percent-encoded paths shown decoded ([image](pr-2068-call-feedback/29-after-routes-endpoints-grouped.jpg)) |

### Test runs on the final tree

- `scripts/ci/test-ui.sh`: green. vitest 235 files and 3,228 tests, style gate green (it was red on the base), promise scan, 203 portable acceptance checks, production build.
- C, ASan and UBSan: all 143 suites through the parallel harness, 8,005 passed, 0 failed, 7 skipped. The watchdog regressions of `scripts/test.sh` Step 5 (parent death, worker supervisor death, worker MCP error) pass.
- clang-format clean on the changed C files. The diff-scoped clang-tidy gate leaves four findings that already apply to the base:
  - the cognitive complexity of `ap_render` (140) and `ap_overview` (67)
  - the file's `calloc(n + 1, ...)` idiom
  - an analyzer path with a NULL file that cannot reach `ap_ownership`, because `ap_load` returns an error first
- DCO: `scripts/check-dco.sh 92224c44..HEAD` reports 49 signed-off commits.

### Not run, and why

- `scripts/test.sh` without `--suites` stops at Step 0x, the packaging version-metadata contract: the package manifests declare 0.10.8 while the newest release is 0.11.0. The same failure occurs on `50a6a67b`, since this branch predates the 0.11.0 release on `main`. The suites and Step 5 were therefore run directly with the same build flags.
- Linux and Windows legs, and cppcheck (not installed locally), were not run.

### Remaining limits and follow-ups

- Django `urls.py` `path()` views are not linked to their routes by HANDLES edges in the index, so only two Django entry points are classified. Behavior covers this with ranked flows; linking the views is an indexer change.
- The listed caller answer is produced in the chat, which needs the browser model to be loaded before a question can be sent.
- Explanations are still worded by the local 0.5B model. The facts it receives are now complete and readable, but the wording quality is bounded by the model.

## Hand test round (3 and 4 October 2026)

Bernhard tested the branch by hand and recorded 28 findings (K1 to K28) in a German correction plan: graph-ui/verification/call-feedback-2026-10-02/KORREKTURPLAN.md (local, not committed), forwarded separately. All 28 are implemented on this branch, each test-first and checked in a headless browser with screenshots. An independent completeness check compared every item with the wanted behaviour; the four gaps it found (K5, K12, K16, K24) were closed in a second round.

Highlights:
- Galaxy: Back/Forward with a bounded shared history (25 steps, recent list, Alt+arrows), empty clicks keep the scope, deep layers load in a few large requests with progress and cancel, a truthful hierarchy (incoming left, outgoing right, mixed directions in their own band), Selection details from the loaded scope.
- Chat: automatic explanations list the indexed facts and send the model only the source; caller questions are answered from the graph, also with typos; configuration files get facts only; earlier answers on another topic are not resent; the model stays loaded across reloads (cached) and project switches (in-page switch).
- Architecture: Back/Forward on the same history model; one green scheme for all scenes; labels clear of each other; Behavior start list, names and counts; honest Service map and route messages.

Browser checks on the final tree: graph-ui/tools/handtest-fixes-galaxy.mjs 29/29, handtest-fixes-architecture.mjs 25/25, handtest-fixes-chat.mjs 19/19 (real model), call-feedback-acceptance.mjs 19/19, handtest-fixes-k27.mjs 55/55. scripts/ci/test-ui.sh green (262 files, 3,570 tests, style gate, promise scan, 203 acceptance checks, build).

Known limits: the 0.5B model can still word things loosely next to the listed facts (its text is labelled, unknown names are flagged); Behavior resolves os.environ.setdefault in manage.py-tpl to a QueryDict method (index call resolution, not frontend); with the chat open the Galaxy toolbar wraps to two rows below about 1,440 px.


## Second hand test round (4 October 2026)

Bernhard tested the branch a second time and recorded 16 findings (K29 to K44) in the same local correction plan. A headless run with the real model covered what he could not run himself (configuration files in Explore, the Endpoints check box, Behavior on cbm) and confirmed what his screenshots showed. Two read only reviewers then went through every change and every chat text the browser had produced; their findings were fixed in a third round. Everything is test first and checked in a headless browser with screenshots.

Galaxy: scene labels stay below the panels, and the panels are opaque so nothing shows through; the node hover card stays above them. Path labels keep off the hierarchy band title and are left out where no free spot exists. Expand +1 explains at once on hover and focus which render limit a layer will likely pass and what the limit does, and while a layer loads it says that "−" cancels it.

Chat: a layer that stopped at the render limit is called partial, with the same wording as the Galaxy tooltip. Counts carry their units ("Incoming: 23 relationships from 12 symbols"), caller lists name the callers first and the other incoming relationships apart, and German answers are German throughout. The note about unknown names ignores case and literals but still flags mangled names, and a dropped model sentence says what it claimed. The model is told the language of the question. Short general questions about a selection, an open code file or marked code get the listed facts, the lines read from the code and at most a checked sentence; a prompt without a question gets example questions; configuration files (YAML, JSON, TOML, INI) are answered with an outline read from the file and a line about what the file is for. "Ask the model" now adds the model's reply below the listed answer instead of replacing it, every free model answer says that it comes from the local model, and returning to a file says "Back to" and sends that file's own earlier turns again.

Architecture: Back and Forward stand first in the subtab row with the same words as in Galaxy and stay in one place in every subtab; the Recent menus of both workspaces close on a press elsewhere, Escape, a choice and any change of place. The Refresh buttons of Architecture, ADR, the project picker and the file impact show that they run and then the time, "no changes" or the error. The Behavior start list has suggestions, then all operations in alphabetical order with look alikes named by class, a filter field and the current start apart from the matches. A project switch keeps the workspace in the address.

Not changed, because correct: "Next lines" is disabled at the end of a file; browser Back and Forward switch projects (the Galaxy Back works inside Galaxy); with the chat open "All graph" reads "All"; older wrong answers in the chat came from the history of the first round.

Checks on the final tree (234e0f23): handtest-fixes-galaxy.mjs 29/29, handtest-fixes-architecture.mjs 25/25, handtest-fixes-chat.mjs 19/19 with the real model, call-feedback-acceptance.mjs 19/19, handtest-fixes-k27.mjs 55/55, all headless. scripts/ci/test-ui.sh is green with 295 test files and 3,858 tests, the style gate, the promise scan, 203 of 203 acceptance checks and the build. The C suites pass with sanitizers: 8,005 passed, 0 failed, 7 skipped in 143 suites.

Known limits: the local 0.5B model stays weak, above all in German, so its sentence is often dropped and an answer shows the facts and the code lines only; its free answers are labelled but not better. General questions about an Architecture selection still go to the model, because those facts exist only in English. Behavior resolves os.environ.setdefault in the Django project template to a QueryDict method, which comes from call resolution in the index. With the chat open the Galaxy toolbar wraps to two rows below about 1,440 pixels.

## Third hand test (4 October 2026, after opening #2545)

Five more findings (K45 to K49), all fixed test first and checked in a headless browser. Questions about the current view ("erkläre die aktuelle Hierarchie", also with typos) are answered from the loaded scope without the model: the root in the middle, what comes in on the left and goes out on the right, by type with names. A model answer that only repeats the question is caught and replaced by the facts of the selection. The git branch node of the index is named for what it is ("django-demo · detached HEAD", "cbm · working tree") instead of a bare "DETACHED". The note about the focus ring shows only beside Explore and says plainly what it means. The search reads every footer the server writes under a full page; before, any search with more than one page fell back to "Index search unavailable". A search hit also ranks by its name in the index, so "working" finds "cbm · working tree" again, and the card of a folder no longer repeats its own path as its place. Checks on the final tree: Galaxy 29/29, Architecture 25/25, chat 19/19 with the real model, acceptance 19/19, Back and Forward in Architecture 55/55; scripts/ci/test-ui.sh green with 308 test files, 3,954 tests and 203 of 203 acceptance checks.
