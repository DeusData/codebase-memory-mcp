# scripts/ci/ — venue plumbing (single implementations)

Support scripts that keep the venues in the SAME shape. Each exists because
the logic used to live inline in workflow YAML or was hand-duplicated between
CI and the local infrastructure — both of which the venue-parity contract
(`tests/test_venue_parity_contract.sh`) now forbids. Everything here answers
`--help` (PowerShell: comment-based help, `Get-Help <script>`).

| script | job | called by |
|---|---|---|
| `new-protected-temp-root.ps1` | Create the owner-stamped, inheritance-protected per-user TEMP root the daemon/install suites require (shared `/tmp` and default runner TEMP grant Authenticated-Users mutation rights, which the trust policy correctly refuses — running there produces security refusals, not signal). `-ProtectDir` stamps build dirs the same way. | `_test.yml`, `_soak.yml`, `vm-run-tests.sh` |
| `clean-test-residue.ps1` | Sweep `cbm-*` residue from the Windows VM and assert runner-like free disk (default 14 GB — the GitHub runner's SSD). Long-path `\\?\` fallback for the guard suites' adversarial trees; every removal VERIFIED (an earlier version counted attempts and reported 86 swept while 11 GB remained). BLOCKS below the floor: a disk that fills mid-run reads as a product bug. | `win.sh` before every build/run |
| `preflight-docker.sh` | Same idea for Colima/docker: prune runner-unlike residue, assert free space on the filesystem backing the docker data root (not the VM's `/`). Build cache + named volumes KEPT (the local analogue of actions/cache); `--deep` drops them. | `test-infrastructure/run.sh` |
| `check-glibc-compat.sh` | Run a linux binary in debian:bullseye (glibc 2.31) — the portable binary must start on old glibc. | `_smoke.yml` portable legs |
| `generate-sbom.py` | The release SPDX SBOM (vendored versions reviewable here, diffable by vendoring PRs — was inline YAML). | `release.yml` |
| `self-index-coverage-gate.sh` | Index the merge base and this branch with the binary just built, and fail the PR only for a finding the branch ADDED (#963): a whole-file parse failure, a range list clipped without saying so, a single range over 25% of a file of 200+ lines, or a flagged-file count above the merge base's. Comparing against the base rather than a fixed number stops main moving the baseline and turning unrelated PRs red (#1972). Reads two data files: `coverage-gate-allowlist.txt` (paths it skips, each with a written reason) and `parse-partial-baseline.txt` (a record it prints, never a gate). Both trees are indexed with the SAME binary, so it cannot see an extractor change that moves both sides together. Ubuntu leg only — the flagged lines depend on which conditional-compilation branches the preprocessor keeps. | `pr.yml pr-smoke` |
| `require-all-green.sh` | The aggregate gate: fail unless every needed job succeeded or legitimately skipped (was inline YAML). | `pr.yml ci-ok` |
| `verify-shard-union.sh` | Prove sharded test legs lost nothing: shard count agreement, indices 1..n, identical suite lists, union of slices == full list (was inline YAML). | `_test.yml` shard-completeness |
| `prepare-release-candidates.sh` | Copy one linker output into stripped/unstripped candidates, finalize signatures, composition-check them without execution, and record their hashes. | `_build.yml`, local artifact smoke |
| `stage-release-candidates.py` | Admit exactly eight candidate artifacts / sixteen byte-distinct binaries into the content-addressed VirusTotal scan set. | `_build.yml` |
| `select-release-candidates.py` | Apply the reviewed tuple-local VT truth table, or the explicit dry-run stripped default, and atomically copy one content-bound binary per target. | `_build.yml` |
| `verify-release-selection.py` | Recompute the selection policy and prove every executable member in all 14 public containers equals its selected SHA-256. | `_build.yml`, `release.yml` final draft verification |
| `check-virustotal.sh` | Poll and validate the exact candidate scan set, enforce engine coverage and the narrow documented Microsoft `!ml` policy, and emit content-bound results evidence. | `_build.yml` |
