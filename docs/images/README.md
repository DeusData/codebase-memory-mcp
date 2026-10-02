# Atlas README captures

These are screenshots and browser recordings of the application, captured on 27 September 2026. No generated interface elements or graph results were added.

## Source and build

- Repository: [DeusData/codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp)
- Branch: `feat/codeatlas-web`, [PR #2068](https://github.com/DeusData/codebase-memory-mcp/pull/2068)
- Commit: `4ff6feeab639c4668da0f491930326ed5ec63093`
- Build: `scripts/build.sh --with-ui --version atlas-4ff6feea`
- Browser: Chrome on macOS, English locale; animations captured at a 1440 × 900 viewport with device scale factor 2
- Agents: standard build, experimental agent activity disabled

The `v0.0.1` label in the application is the frontend package version. It is not the backend release version. This branch was an unmerged development preview when recorded.

## Data shown

The project `codebase-memory-mcp` is a sparse checkout of the same public commit containing root files, `src/`, `graph-ui/src/`, and `tests/`. It was indexed in `full` mode into an isolated local cache. No private repository was used.

The index reported 24,770 nodes, 136,806 edges, and 63 partially parsed files. These are properties of this selected source scope, not a benchmark of the whole repository. The architecture map loads 20,000 source nodes and 67,053 edges; the Galaxy capture loads 5,000 nodes and 5,353 relationships. The interface retains its coverage and display-limit notices.

## Media

| File | What it shows |
|------|---------------|
| [atlas-architecture.webp](atlas-architecture.webp) | Zooming and orbiting the map, selecting `src/graph_buffer`, opening that area, selecting `graph_buffer.c`, and opening its source evidence. |
| [atlas-architecture.png](atlas-architecture.png) | Still image of the same view. |
| [atlas-galaxy.webp](atlas-galaxy.webp) | Orbiting the loaded code graph and its coverage shadow. |
| [atlas-galaxy.png](atlas-galaxy.png) | Still image of Galaxy. |
| [atlas-explore.png](atlas-explore.png) | `hotspot-map.ts` in the IDE-style reader, with the file tree and graph alongside the source. |
| [atlas-chat.png](atlas-chat.png) | A real Qwen2.5 Coder 0.5B response about the selected `hotspotIdentity` source, beside the reader. |
| [atlas-system-structure.png](atlas-system-structure.png) | Component groups and indexed dependencies in System structure. |
| [atlas-behavior.png](atlas-behavior.png) | Possible static calls from `SpatialArchitecture`, with source evidence. |

The animations were captured as PNG browser frames at 2880 × 1800, then encoded as animated WebP. Architecture is 2880 × 1800 and 16.67 seconds, encoded at 15 frames per second with unchanged frames combined. Galaxy is 2400 × 1500 and 10.58 seconds, with 104 frames retaining their recorded timing. Architecture uses WebP quality 94; Galaxy uses quality 90 and independent frames to avoid trails from frame compositing. No GIF palette reduction was applied. The corresponding PNG stills retain 2880 × 1800 pixels. Explore and Chat screenshots are 3200 × 2200; the additional System structure and Behavior screenshots retain their original 1600 × 1000 resolution.

The application data and text were not retouched. Directional pulses represent indexed relationships, not observed execution. Captures were inspected as decoded frames and in the rendered README for sharpness, camera movement, readable source, and unwanted overlays. The browser reported no JavaScript page errors during capture.

## Local chat check

The Chat screenshot uses the actual browser model, downloaded through the application's own setup. Its source context is the selected `hotspotIdentity` definition at `graph-ui/src/architecture/hotspot-map.ts:18:1-19:1`. The request used 293 input tokens. The response is shown as generated; it is not an edited or fabricated transcript.

A previous request using the whole file (2,641 input tokens) failed with `memory access out of bounds`. The later selection request succeeded. This is a limitation observed in the branch preview, not a fixed product bug or a general model-performance claim.
