# GDScript Good-Tier Checklist

## Required evidence gates

- [x] `docs/superpowers/proofs/gdscript-good-tier-manifest.json` is the committed approval-bearing source for the exact 4 pinned proof targets used in v1.
- [x] Each approved manifest entry records canonical identity as `remote` + `pinned_commit` + `project_subpath` when needed + recorded `godot_version`.
- [x] Approved manifest entries are explicitly marked as canonical and Godot 4.x qualifying rather than relying on implied status.
- [x] The curated set includes at least one small/simple repo and at least one repo with meaningful cross-file behavior.
- [x] The curated repos collectively exercise indexing, same-script calls, signal calls, imports/preloads, and inherits.
- [x] `scripts/gdscript-proof.sh` can classify each assertion as `pass`, `fail`, or `incomplete`.
- [x] `scripts/gdscript-proof.sh` can classify the overall run as `pass`, `fail`, or `incomplete`.
- [x] The aggregate proof exits successfully only when all gating assertions pass.
- [x] Missing required repos, pin mismatches, or non-comparable inputs force the run to `incomplete`.
- [x] Every resolved proof miss is covered by the narrowest appropriate regression layer.
- [x] `make -f Makefile.cbm cbm` passes from `.worktrees/gdscript-support/`.
- [x] `make -f Makefile.cbm test` passes from `.worktrees/gdscript-support/`.
- [x] `scripts/gdscript-proof.sh` remains the only approval-bearing workflow for Phase 03 review.
- [x] Approval evidence covers all four manifest repos: `squash-the-creeps`, `webrtc-signaling`, `webrtc-minimal`, and `topdown-starter`.
- [x] Approval evidence includes both `sequential` and `parallel` runs for every manifest repo.
- [x] `semantic-parity.json` and `semantic-parity.md` are present and reviewed for every approval-bearing manifest run.
- [x] `SEM-01` review requires non-zero class and method counts plus representative class and method samples.
- [x] `SEM-06` review is semantic parity across counts plus representative samples/edges, not byte-for-byte artifact equality.
- [x] When parity results are `incomplete`, operators inspect `run-index.json`, paired `repo-meta.json`, and wrapper `queries/*.json` before making approval decisions.

## Current notes

- The current manifest captures 4 pinned proof targets across 2 remotes. Three targets share `godot-demo-projects.git` and are distinguished by `project_subpath`.
- Manifest mode is the only approval-bearing lane; ad hoc `--repo` runs remain debug/investigation only and must stay non-canonical/non-qualifying.
- Manifest mode now has fresh evidence for `pass`, `fail`, and `incomplete`.
- Fresh aggregate proof pass for all manifest assertions: `.artifacts/gdscript-proof/20260408T043206Z-14613-9mUP0e`.
- Dedicated proof-harness regression now covers the closed `calls.same_script_edges` miss: `scripts/test_gdscript_proof_same_script_calls.py` runs under exact `make -f Makefile.cbm test`.
- Phase 03 semantic review is now counts-plus-samples across the four pinned repos in both sequential and parallel mode.
- `semantic-parity.json` is the machine-readable parity surface; `semantic-parity.md` is the human-readable rollup; wrapper `queries/*.json` stay canonical raw evidence.
- Aggregate `pass` only supports `Promotion answer: qualified-support-only` for the approved manifest corpus on the current commit under test.
- Aggregate `fail` and aggregate `incomplete` both require `Promotion answer: do-not-promote` until the blocking issues are resolved.
- GDScript promotion is now justified for manifest-validated README wording only when the summary uses the qualified-support-only contract, not as a scored benchmark language in `docs/BENCHMARK.md`.

## Promotion gate

- [x] For aggregate `pass`, `aggregate-summary.md` says `Promotion answer: qualified-support-only`.
- [x] For aggregate `pass`, `aggregate-summary.md` also says `Claim scope: approved manifest corpus only; current commit only`.
- [x] For aggregate `fail`, `aggregate-summary.md` says `Promotion answer: do-not-promote`.
- [x] For aggregate `incomplete`, `aggregate-summary.md` says `Promotion answer: do-not-promote`.
- [x] Each repo `summary.md` includes `## Verdict`, `Repo verdict: ...`, and `Approval contribution: ...` before the evidence is used for promotion review.
- [x] README language-tier wording is updated only when the manifest-driven proof supports `qualified-support-only`.
- [x] `docs/BENCHMARK.md` is updated only if the scored-tier claim is actually justified beyond the qualified-support-only contract.
- [x] Remaining limitations are documented honestly.
