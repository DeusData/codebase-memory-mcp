# CBM Lens Beta README captures

These are screenshots and browser recordings of the application, captured on 12–13 October 2026. The application data and text were not retouched. Bernhard Jackiewicz captured the recordings and PNG stills on 12 October. The Explorer and Galaxy JPEG stills were refreshed on 13 October. One addition is drawn on top of the recordings: a small mouse pointer and click ring, because a headless browser shows no pointer of its own.

## Original captures: 12 October

- Repository: [DeusData/codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp)
- Branch: `main`, after [PR #2068](https://github.com/DeusData/codebase-memory-mcp/pull/2068) brought CBM Lens Beta in
- Commit: `4d9592b5`
- Build: `scripts/build.sh --with-ui`
- Browser: headless Chromium (Playwright) on macOS, English locale, viewport 1440 × 900, device scale factor 1
- Agents: standard build, experimental agent activity disabled

The `v0.0.1` label in the application is the frontend package version. It is not the backend release version.

### Data shown

The project `codebase-memory-mcp` is an export of the same public commit (`git archive`), indexed in `full` mode into an isolated local cache. No private repository was used. Because the export has no `.git` folder, Explore reports "Git unavailable" in its file impact line.

The index reported 75,037 nodes and 361,349 edges. These are properties of this source scope, not a benchmark. The interface keeps its coverage and display-limit notices.

## Refreshed stills: 13 October

`lens-explore.jpg` and `lens-galaxy.jpg` are unedited browser captures at 1280 × 720 from the locally served CBM Lens Beta UI. The running binary reports `dev`; its exact build commit is unavailable, so these captures do not certify an exact source revision. The local project is named `cbm-triage-main-20260919`; the screenshots show public CBM source and indexed relationships.

- **Explore:** `src/foundation/limits.c`, its compact file impact summary and direct dependencies (24 nodes, 26 edges). The UI's freshness, sampling and coverage caveats remain visible.
- **Galaxy:** `cbm_max_file_bytes`, both directions, all edge types, two layers (160 nodes, 218 edges).

These counts describe the displayed index snapshot, not a benchmark or proof of complete repository coverage. The original recordings and other stills are unchanged.

## Media

| File | What it shows |
|------|---------------|
| [lens-overview.gif](lens-overview.gif) | Architecture overview of the repository: zoom and a small orbit, selecting `src/pipeline`, opening that area, selecting `artifact.c` and opening its source. |
| [lens-architecture.png](lens-architecture.png) | Still image of the whole repository on the architecture map. |
| [lens-galaxy.gif](lens-galaxy.gif) | Galaxy: searching `cbm_pipeline_run`, following its outgoing calls one layer further, the hierarchy view and the path to `cbm_store_check_integrity`. |
| [lens-galaxy.jpg](lens-galaxy.jpg) | Two dependency layers around `cbm_max_file_bytes`, with direction and edge-type controls. |
| [lens-explore.jpg](lens-explore.jpg) | `src/foundation/limits.c` in the reader with its file impact summary, file tree and direct graph dependencies. |
| [lens-chat.gif](lens-chat.gif) | The chat with the local Qwen2.5 Coder 0.5B model: the explanation of `cbm_pipeline_pass_doclinks` and "Who calls cbm_pipeline_pass_doclinks?" answered from the graph. |
| [lens-chat.png](lens-chat.png) | Still image of the same explanation and listed answer. |
| [lens-system-structure.png](lens-system-structure.png) | Component groups and their dependencies in System structure, `src` selected. |
| [lens-behavior.png](lens-behavior.png) | Possible direct calls of `compileCards`, with its declaration. |

The recordings were captured as browser video, then encoded as GIF at 960 pixels wide and 10 frames per second with a palette per recording. While a search is typed, the Galaxy briefly shows "No matching indexed nodes." until the index search answers; those frames (under one second) were cut. The original PNG stills are 1440 × 900, the chat still 1440 × 1200 so that the explanation and the answer fit together. The refreshed JPEG stills are 1280 × 720. Directional pulses represent indexed relationships, not observed execution.

## Local chat check

The chat recording uses the actual browser model, downloaded through the application's own setup. The explanation lists the indexed facts and the declaration of the selected function; the one sentence after them was written by the model and is labelled as such. The caller answer is listed from the indexed graph without the model.
