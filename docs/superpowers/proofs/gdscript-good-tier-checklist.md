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

## Current notes

- The current manifest captures 4 pinned proof targets across 2 remotes. Three targets share `godot-demo-projects.git` and are distinguished by `project_subpath`.
- Manifest mode is the only approval-bearing lane; ad hoc `--repo` runs remain debug/investigation only and must stay non-canonical/non-qualifying.
- Manifest mode now has fresh evidence for `pass`, `fail`, and `incomplete`.
- Fresh aggregate proof pass for all manifest assertions: `.artifacts/gdscript-proof/20260408T043206Z-14613-9mUP0e`.
- Dedicated proof-harness regression now covers the closed `calls.same_script_edges` miss: `scripts/test_gdscript_proof_same_script_calls.py` runs under exact `make -f Makefile.cbm test`.
- GDScript promotion is now justified for manifest-validated README wording, but not as a scored benchmark language in `docs/BENCHMARK.md`.

## Promotion gate

- [x] README language-tier wording is backed by the manifest-driven proof.
- [x] `docs/BENCHMARK.md` is updated only if the scored-tier claim is actually justified.
- [x] Remaining limitations are documented honestly.
