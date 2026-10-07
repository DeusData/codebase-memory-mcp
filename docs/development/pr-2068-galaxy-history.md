# PR 2068: back and forward in Galaxy (K2) and Architecture (K27)

Both workspaces share one bounded history model and one set of rules, and each
keeps its own history. Galaxy came first and is described first; the section
"Architecture (K27)" lists what an Architecture entry holds and the few rules
Architecture adds.

## Galaxy (K2)

Hand test finding K2: a click on a node makes it the new root, and the only way
out is "All graph". This note fixes the concept before the code. K9 belongs to
it: a click on empty canvas no longer leaves the scope, so Back is the way to
undo a step.

### What an entry is

One entry is the complete question the Galaxy workspace answers at that moment:

| Field | Meaning |
|---|---|
| `scope` | The root identity: node id, qualified name and display name, or a file or folder path. Missing means "All graph". |
| `depth` | Loaded layers (hops). |
| `direction` | `both`, `inbound` or `outbound`. |
| `edgeTypes` | The traced relationship types, or all. |
| `trail` | The open path target (`path` to node id) or `calls` (call order), if one is shown. |
| `mode` | `galaxy` or `hierarchy`. |

The entry holds identities and parameters only, never loaded nodes, edges,
layouts or camera positions. Going back re-runs the scope; the bounded scope
cache (four results) and the neighbourhood cache make that fast in practice,
and nothing grows with the session.

"All graph" is an entry like any other, so Back after "All graph" returns to
the scope that was open before.

### Rules

1. **Bounded.** At most 25 entries. A push beyond that drops the oldest entry.
2. **Consecutive duplicates merge.** An entry equal to the current one (same
   key over all fields) is not pushed. Revisiting the same root later is a new
   entry, because it is a new step.
3. **Browser semantics.** Back and Forward move a cursor. A new navigation after
   going back drops the forward branch.
4. **No browser history per step.** There is no `pushState` per step: a long
   session would leave thousands of browser entries, and the browser Back button
   would no longer leave the page. The address bar keeps its current role.
5. **Recent roots.** Besides the linear history a list of the last 8 distinct
   roots (newest first) allows a direct jump. A jump is a new navigation: it
   restores that root with its last depth, direction and edge types and drops the
   forward branch. The list is independent of the cursor, so going back does not
   shorten it.
6. **Keyboard.** Alt+Left is Back, Alt+Right is Forward, not while typing in a
   field or the editor, and not while another surface takes the keys
   (`escapeTaken`). The page prevents the browser default for these keys.
7. **Escape.** Escape first clears an open path, then leaves the scope ("All
   graph"), which is itself a history entry.
8. **Per project.** A project switch starts a fresh history. A project that
   arrives after the first render (from the address bar) keeps "All graph" as
   the first entry.
9. **Cancel is Back.** The minus button while a layer loads (K8) moves the
   cursor back to the previous depth instead of pushing it again, so Forward
   retries the cancelled layer.
10. **Empty canvas.** A click on empty canvas inside a scope clears only the
    path highlight (K9). That is a step of its own, so Back brings the path
    back.
11. **Selection follows the root.** Back or Forward to "All graph" clears the
    selection. An entry with another root selects that root once it is loaded.
    An entry with the same root (another depth, direction or path, and a
    cancelled layer) keeps the selection, so Selection details and the chat
    context stay.

### Where it lives

- `graph-ui/src/graph/navigation-history.ts`: the generic, pure model
  (`push`, `back`, `forward`, `recent`) over any entry with a key function.
  Architecture (K27) uses the same module, see below.
- `graph-ui/src/galaxy/scope-history.ts`: the Galaxy entry, its key and the
  tooltips that name a target ("Back to JSONBAgg · 2 layers").
- `GalaxyPanel.tsx`: derives the current entry from its state, pushes it when it
  changes, restores an entry on Back, Forward or a recent jump, and shows the
  Back and Forward buttons with disabled states and the Recent list in the
  scoped toolbar.

### Not in scope

- No breadcrumb of the whole chain. The tooltips name the Back and Forward
  targets, and the Recent list names the roots; a breadcrumb would cost the one
  toolbar row that K3 asks for.
- No persistence across reloads. A reload starts with an empty history, the same
  as a new browser tab.

## Architecture (K27)

Hand test finding K27: Architecture had several ways back that meant different
things. Overview had its location trail ("django-demo / django"), System
structure had a "← Back" that walked the focus, Behavior had a "← Back" that
walked the starts, and "Show these N routes" had no way back at all. Nothing
went forward. Architecture now uses the same model (`navigation-history.ts`)
and the same rules as Galaxy, with a history of its own.

### What an entry is

One entry is the place the workspace shows: the subtab and what that subtab
has opened.

| Field | Meaning |
|---|---|
| `view` | The subtab: Overview (Structure, or Entry points as its mode), Routes, Hotspots, System structure or Behavior. |
| `spatial` | Overview, Entry points, Hotspots and Endpoints draw one map: the opened area (`areaPath`) or file (`filePath`), the hotspot area, and Plan or 3D (`planar`). |
| `routes` | The Routes perspective: Service map or Endpoints. |
| `filter` | The Routes filter. An opened route group ("Show these N routes") is that filter. |
| `system.focus`, `system.expanded` | The System structure focus and the expanded groups. |
| `system.structurePlanar`, `system.behaviorPlanar` | Plan or 3D in System structure and in Behavior. Each keeps its own camera, as before K27. |
| `system.behavior` | The Behavior start, the destination ("Reach") and, after a followed call (double-click, "Follow calls from here"), the start the hops began from. |
| `system.behavior.position` | Where the journey stands: the indexed path to the destination ("Path 2"), the operation on that call chain, and the page of direct calls. |
| `system.shown` | The start Behavior shows when none was requested (the server picks one, or the first entry point), so a tooltip can name it. |

The entry holds identities, names and a few small numbers, never projections,
scenes or cameras. Its key covers only what the current subtab shows: a field
another subtab holds does not split a step, and the names are there for the
tooltips only. The Behavior start is the numeric identity of one analysis
snapshot and stays guarded by its generation, as before.

What another subtab holds stays out of that subtab entirely, not only out of
the key. The map reads the opened area or file only in Overview and the hotspot
area only in Hotspots, so Entry points and Endpoints neither draw nor report an
area opened in Overview, and the chat context there names nothing that is not
on screen. Back to Overview opens the area again.

Every view that draws a scene makes Plan or 3D a step: the shared map, System
structure and Behavior. Another path to a destination is a step too. Walking
the call chain (Previous, Next, the slider, a click on an operation) and paging
through direct calls are no steps, because Previous and Next already do that;
the entry keeps where the journey stood, so Back and Forward return to the same
operation. A new start, destination or followed call begins at the top of its
journey.

### Rules

All Galaxy rules apply: at most 25 entries, consecutive duplicates merge, a new
navigation after Back drops the forward branch, no `pushState` per step, a
Recent list of the last 8 distinct places that is independent of the cursor,
Alt+Left and Alt+Right neither while typing nor while another surface takes the
keys, and a fresh history per project (the workspace remounts per project).
Architecture adds:

1. **One Back.** Back, Forward and Recent sit beside the subtabs and serve the
   whole workspace. The "← Back" buttons System structure and Behavior had in
   their own toolbars are absorbed into it: there is one Back control with one
   meaning, and a step in System structure or Behavior is one Back away like
   any other. "Whole system" stays as a way to the top, and the Overview
   location trail stays; both are ordinary steps.
2. **Separate from Galaxy.** Galaxy and Architecture each keep their own
   history, and Alt+Left and Alt+Right act only in the active workspace.
3. **Typing is one step.** Typing in the Routes filter becomes a step once it
   pauses (600 ms), not one step per key. A navigation, Back, Forward or a
   Recent jump while typing first records what the field showed, so typed text
   is never lost: Back leaves it for the place before the typing and Forward
   returns to it. Forward while typing finds no forward branch, because the
   text is a new navigation; it records the text and stays.
4. **The page's own choices are no steps.** What the page sets by itself (the
   suggested Behavior start such as `main`, a reset after reindexing) replaces
   the current step (`replaceNavigation`). As a step of its own, Back would land
   on an empty Behavior, the page would pick `main` again, and Back would never
   get past it. A replaced step that equals the step before or after it merges
   with that step, so Back and Forward never lead to the same place twice.
5. **Followed calls.** A followed call is a new start that remembers where the
   hops began. Empty background returns there, as it did before, and that is a
   step too.
6. **Tooltips name the target**, for example "Back to Overview · django",
   "Forward to Routes · Endpoints · /edit" or "Back to Behavior · get_autocommit
   · followed from handle".
7. **The current step keeps its newest details.** A change that leaves the key
   alone (a name known only later, a field another subtab holds) updates the
   current entry in place (`refreshNavigation`) instead of adding a step. On
   django-demo the suggested `main` is rejected once for a stale analysis
   snapshot, the start is reset, and the journey shows `main` on its own; the
   step is still named "Behavior · main".

### Where it lives

- `graph-ui/src/graph/navigation-history.ts`: the shared model, with
  `replaceNavigation` for rule 4 and `refreshNavigation` for rule 7.
- `graph-ui/src/architecture/architecture-history.ts`: the Architecture entry,
  its key, its recent place and the labels for tooltips and the Recent list.
- `graph-ui/src/architecture/use-architecture-history.ts`: the place as state,
  pushing on a new key, restoring without a push, the filter pause and the keys.
- `ArchitecturePanel.tsx` holds the place and shows Back, Forward and Recent.
  `SpatialArchitecture`, `RoutesArchitecture`, `SystemArchitecture` and
  `BehaviorJourney` read their part of the place and report changes back
  (`lifted-place.ts`). Rendered on their own, as in their unit tests, they keep
  that part as their own state; none of them has a Back of its own.
- Tests: `architecture-history.test.ts` (entry, key, labels),
  `ArchitecturePanel.history.test.tsx` (the wiring) and the browser run
  `graph-ui/tools/handtest-fixes-k27.mjs` on django-demo and cbm.

### Not in scope

- Neither steps nor kept in the entry: the Entry points start and call depth,
  selections, the relationship filters, the System structure checkboxes, and
  the camera position (zoom, pan, "Fit"). Plan or 3D is a step, the camera
  position is not.
- No persistence across reloads. As before, only the subtab is remembered per
  project.
