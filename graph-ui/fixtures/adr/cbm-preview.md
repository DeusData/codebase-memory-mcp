# Architecture decisions

> Preview examples for cbm-preview. These editable notes capture frontend design choices discussed during development; they are not a historical ADR archive or graph-inferred facts. Status: proposed for review.

## 001 — Explain architecture from evidence

**Status:** Proposed · **Scope:** Architecture workspace

### Context

A repository map explains where code lives. System structure and behavior also need to explain relationships, boundaries, and execution steps. A convincing picture should remain traceable to its inputs.

### Decision

Combine indexed graph relationships with source evidence. Keep the repository map, routes, hotspots, system structure, and behavior as distinct lenses. Mark inferred relationships as inferred, and make their evidence inspectable. Do not embed project-specific architectural explanations in the visualization code.

### Consequences

- The same analysis can be applied to other indexed repositories.
- Missing evidence remains a visible limitation rather than a fabricated connection.
- A source or graph change can alter the view; these written decisions change only when someone edits them.

**Implementation references:** `graph-ui/src/architecture/`, `src/store/architecture_projection.c`.

## 002 — Keep browser AI optional

**Status:** Proposed · **Scope:** Local chat

### Context

Code exploration should work before any model is downloaded. Users may want to ask about a file or a literal code selection without sending source to a hosted model.

### Decision

Run the chat model locally in the browser after explicit setup. Download weights only after opt-in. Attach the selected code or graph context to the next prompt and render answers as Markdown.

### Consequences

| Benefit | Tradeoff |
| --- | --- |
| Exploration works without model setup | Chat is unavailable until a model is enabled |
| Inference stays on the device | Model quality and speed depend on local resources |
| The prompt can use exact marked code | A bounded context cannot include an entire large repository |

**Implementation references:** `graph-ui/src/browser-ai/`, `graph-ui/src/reader/MonacoReader.tsx`.

## 003 — Keep agent activity experimental

**Status:** Proposed · **Scope:** Agents workspace

### Context

The current integration records events from explicitly configured agent hooks. The UI cannot infer every agent operation merely by observing the graph.

### Decision

Hide Agents unless the experimental build flag is enabled. Treat hook events as observed activity, and avoid implying complete task execution, tool outcomes, or duration data.

### Consequences

The default navigation remains focused. Further agent adapters and a richer event contract can be introduced before promoting the view to a standard workspace.

**Implementation references:** `graph-ui/src/app/feature-flags.ts`, `graph-ui/agents/hooks/atlas-trace.py`, `src/ui/activity.c`.
