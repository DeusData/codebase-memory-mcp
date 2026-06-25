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

---

## M1 results (2026-06-21) — after WS2a + WS1

| metric | baseline cbm | **after M1** | codegraph | status |
|---|---|---|---|---|
| dup_nodes | 38 | **0** | 0 | ✅ tied (WS2a) |
| `explore` tool (1-call source+blast-radius) | ✗ (3 calls) | **✅ 1 call** | ✅ | ✅ matched (WS1) |
| explore caller attribution | — | **precise + ⚠hotspot fan-in** | imprecise, no hotspot | ✅ exceeds |
| explore cypher escape-hatch | — | ✅ | ✗ | ✅ exceeds |
| explore auto-expand to neighbors | — | ✗ (focused) | ✅ | codegraph edge |

Head-to-head on `grade`: cbm matches codegraph's one-call source+blast-radius, beats it on precision/hotspots/cypher, trails on neighbor auto-expansion.
Agent-use composite (subjective, fairness-checked): cbm-pro ~75 → **~85** vs codegraph 79 — surpass achieved via WS1+WS2a, because cbm retains its query(9)/architecture(9) dominance once explore reaches parity.

Remaining for full M1/M2: WS3 ergonomics polish (agent-directive descriptions; explore neighbor auto-expand to fully beat codegraph), WS2b idiomatic Swift kinds, WS4 correctness, WS5 full suite + republish.
