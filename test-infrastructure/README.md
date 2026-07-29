# test-infrastructure/ — the local venues

The local half of the 3-OS CI ladder. **Every leg here runs the SAME canonical
scripts CI runs** (`scripts/test.sh`, `build.sh`, `smoke-local.sh`,
`soak-legs.sh`, `vm-smoke.sh` — see `scripts/README.md`); this directory only
provisions containers and the Windows VM. The venue-parity contract
(`tests/test_venue_parity_contract.sh`, Step 0j of every test leg) fails the
build if a venue grows its own harness logic — here or in the workflows.

## Entry points

| venue | entry | notes |
|---|---|---|
| Linux (arm64 + amd64) + cross-compile | `./run.sh <leg>` (`--help` for the leg list) | Colima only — Docker Desktop is broken on this machine. A clean-disk preflight (`scripts/ci/preflight-docker.sh`) runs before every leg: GitHub runners are ephemeral with a known-free 14 GB disk, so the long-lived VM is swept back to that shape first. |
| Real Windows (UTM ARM64 VM) | `vm/win.sh <command>` (`win.sh help`) | Same preflight idea (`scripts/ci/clean-test-residue.ps1`), plus CI's protected per-user TEMP root. Wine (`run.sh windows`) is a compile check only — never a substitute for the VM. |
| macOS | run the canonical scripts natively | `scripts/test.sh`, `scripts/build.sh`, `scripts/smoke-local.sh …` |

## The ladder (before any push)

1. `scripts/test.sh` — macOS native (or `--suites …` while iterating)
2. `./test-infrastructure/run.sh full` — Linux arm64 test/build/TSan/smoke/portable + mingw cross-compile (+ `amd64`, `soak-linux` as needed)
3. `vm/win.sh test-par` · `guards` · `smoke-install` · `soak` — the real-Windows legs

Infra unavailable = a run blocker to escalate — never a silent bypass.

## Fidelity guarantees (what makes local predictive of CI)

- **Same scripts, same sequence, same flags** — platform specifics (e.g. the
  CLANGARM64 trap-UBSan default) live inside the canonical entries, applied
  identically everywhere.
- **Same environment shape** — protected TEMP roots, sandboxed smoke, clean
  builds with the content-verified ccache (a hit is byte-identical to a cold
  compile; caches only accelerate, never change results).
- **Same starting disk** — preflights sweep residue and BLOCK below the
  runner's 14 GB floor (a quietly full disk fails inside install paths and
  masquerades as a product bug).
- **Known, deliberate residuals** — runner physics (4 vCPU vs local cores,
  shared tenancy), Defender ON in the VM vs OFF on GitHub runners, no TSan on
  Windows anywhere, no ASan runtime on native ARM64 Windows (trap-UBSan +
  PageHeap stand in).

See `vm/README.md` for VM provisioning and day-to-day VM mechanics.
