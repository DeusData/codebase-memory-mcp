# Requirements: GDScript Support Verification

**Defined:** 2026-04-11
**Core Value:** A real Godot demo project indexes cleanly and yields useful GDScript answers through the parser-to-MCP path.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Proof Contract

- [x] **PROOF-01**: Maintainer can run validation against pinned real Godot 4.x demo targets with recorded repo identity and project subpath when needed
- [ ] **PROOF-02**: Maintainer can execute a fixed MCP query suite for each proof target after indexing completes
- [ ] **PROOF-03**: Each proof target produces an explicit `pass`, `fail`, or `incomplete` outcome instead of informal notes

### GDScript Semantics

- [ ] **SEM-01**: Maintainer can confirm non-zero GDScript class and method extraction from a real proof target with reviewable samples
- [ ] **SEM-02**: Maintainer can confirm same-script method calls resolve on a real proof target
- [ ] **SEM-03**: Maintainer can confirm queryable `extends` inheritance relationships on a real proof target
- [ ] **SEM-04**: Maintainer can confirm queryable `.gd` dependency relationships from `preload` or `load` targets on a real proof target
- [ ] **SEM-05**: Maintainer can confirm signal declarations and conservative signal-call behavior on a real proof target
- [ ] **SEM-06**: Maintainer can confirm the core GDScript behaviors above remain consistent across sequential and parallel indexing paths

### Proof Evidence

- [ ] **EVID-01**: Maintainer can run proof validation with isolated local state under repo-owned artifacts so results are reproducible on the same machine
- [ ] **EVID-02**: Maintainer can inspect machine-readable raw query artifacts for each proof target
- [ ] **EVID-03**: Maintainer can inspect a per-repo summary that explains what passed, failed, or remained incomplete

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Proof Management

- **PMGT-01**: Maintainer can run a manifest-driven curated proof lane across multiple pinned Godot repositories
- **PMGT-02**: Maintainer can compute aggregate acceptance across multiple repos with per-category gating rules
- **PMGT-03**: Maintainer can run a lightweight automated smoke check for the proof harness in CI without depending on heavyweight external-repo execution

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Generic Godot app or gameplay validation | This milestone proves indexing and MCP query usefulness, not whether demo games run correctly |
| New graph schema or signal-specific edge types | v1 should prove GDScript support through the existing graph model instead of expanding it |
| Non-GDScript language expansion | The current project goal is narrowly focused on GDScript verification |
| Godot 3.x compatibility work | The validation contract is scoped to Godot 4.x proof targets |
| Asset graph modeling for scenes, resources, or textures | Non-code asset semantics are not required to prove parser-through-MCP GDScript support |
| Broad engine semantic inference beyond documented v1 behavior | This would overclaim support before the core contract is proven |
| UI redesign or visualization work | Existing MCP and proof-summary outputs are sufficient for this milestone |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| PROOF-01 | Phase 1 - Proof Contract & Corpus | Complete |
| PROOF-02 | Phase 2 - Isolated Proof Harness | Pending |
| PROOF-03 | Phase 4 - Verdicts & Acceptance Summaries | Pending |
| SEM-01 | Phase 3 - Real-Repo Semantic Verification | Pending |
| SEM-02 | Phase 3 - Real-Repo Semantic Verification | Pending |
| SEM-03 | Phase 3 - Real-Repo Semantic Verification | Pending |
| SEM-04 | Phase 3 - Real-Repo Semantic Verification | Pending |
| SEM-05 | Phase 3 - Real-Repo Semantic Verification | Pending |
| SEM-06 | Phase 3 - Real-Repo Semantic Verification | Pending |
| EVID-01 | Phase 2 - Isolated Proof Harness | Pending |
| EVID-02 | Phase 2 - Isolated Proof Harness | Pending |
| EVID-03 | Phase 4 - Verdicts & Acceptance Summaries | Pending |

**Coverage:**
- v1 requirements: 12 total
- Mapped to phases: 12
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-11*
*Last updated: 2026-04-11 after roadmap creation*
