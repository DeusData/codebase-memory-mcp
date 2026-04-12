---
status: passed
phase: 01-proof-contract-and-corpus
verified: 2026-04-12
requirements:
  - PROOF-01
score: 3/3
---

# Verification: Phase 01 Proof Contract & Corpus

## Goal Check

Phase 1 goal: maintainers can run verification against a pinned, comparable set of real Godot 4.x proof targets with unambiguous repo identity and project subpath handling.

## Must-Haves

1. **Approved proof targets are manifest-defined, not ad hoc.**
   - Passed: `gdscript-good-tier-manifest.json` now declares `approval_mode: manifest-only` and explicit canonical/qualification metadata.
2. **Each target exposes rerunnable identity metadata.**
   - Passed: the manifest and proof output use `remote`, `pinned_commit`, `project_subpath` when needed, and recorded `godot_version`.
3. **Only explicit Godot 4.x targets count for v1 approval.**
   - Passed: docs, checklist, and generated proof artifacts distinguish qualifying manifest runs from non-qualifying ad hoc runs.

## Automated Checks

- `python3 -c "import json, pathlib; data=json.loads(pathlib.Path('docs/superpowers/proofs/gdscript-good-tier-manifest.json').read_text()); assert len(data['repos'])==4; assert data['approval_contract']['approval_mode']=='manifest-only'; assert all(repo['canonicality']=='canonical-approved-v1-target' for repo in data['repos']); assert all(repo['qualification']['qualifies_for_v1'] for repo in data['repos'])"`
- `python3 scripts/test_gdscript_proof_same_script_calls.py`
- `python3 scripts/test_gdscript_proof_manifest_contract.py`

## Result

Passed. Phase 1 now has both a committed approval contract and executable proof-harness guardrails for canonical versus ad hoc runs.

## Human Verification

None required.
