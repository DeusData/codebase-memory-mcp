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
| `require-all-green.sh` | The aggregate gate: fail unless every needed job succeeded or legitimately skipped (was inline YAML). | `pr.yml ci-ok` |
| `verify-shard-union.sh` | Prove sharded test legs lost nothing: shard count agreement, indices 1..n, identical suite lists, union of slices == full list (was inline YAML). | `_test.yml` shard-completeness |
| `check-virustotal.sh` | Release-asset VirusTotal lookups. | `release.yml` |
