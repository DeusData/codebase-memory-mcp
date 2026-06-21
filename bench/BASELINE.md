# Head-to-head baseline — cbm-pro (24e6784c) vs codegraph (0.9.9)

Repo: LingoLearn-iOS-main (29 Swift files). Harness: `bench/headtohead.sh`. Date: 2026-06-21.
Per "confirm the failure before fixing it" — this is the *before* state. Re-run after each WS to prove movement.

## Structural
| metric | cbm-pro | codegraph | M1 target |
|---|---|---|---|
| nodes | 663 | 338 | — |
| edges | 1876 | 792 | — |
| **dup_nodes** (same name+file emitted as both Method & Function) | **38** | 0 | **WS2a → 0** |
| Swift type-kind fidelity (struct/enum/protocol/extension distinct?) | **1** (all → `Class`) | 5 | WS2b (M2) → ≥5 |

## Call-graph parity (callers; grep is a noisy upper bound)
| symbol | cbm | codegraph |
|---|---|---|
| makeInMemoryContext | 16 | 16 |
| makeWord | 12 | 12 |
| Date / Color / tap | diverge (stdlib-constructor counting) | — |
→ roughly at parity; not where M1 moves.

## Ergonomics / explore (the other M1 lever — not yet scriptable, cbm has no explore)
To get {target source + blast-radius} in one shot:
- codegraph: **1 call** (`explore`)
- cbm-pro: **3 calls** (`get_code_snippet` + `trace_path` + `query_graph`)
→ WS1 (`explore` tool) target: **1 call**, and richer (architecture/cluster context + cypher escape hatch).

## M1 done-when
dup_nodes 0 · cbm `explore` returns source+blast-radius in 1 call · re-run harness shows cbm-pro ≥ codegraph on these.
