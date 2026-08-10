# scripts/ — the canonical leg entries

**The doctrine (enforced, not advisory):** *a venue may provision a machine;
only a canonical leg script may exercise the product.* Local CI, PR CI, the
dry run and the release all call the SAME files below — venues differ only in
host specs, architecture and inputs, never in what a leg does. Platform
differences (arm64 sanitizer flags, native Windows behavior, the Linux portable
binary) live INSIDE these scripts, once, never per-venue.
`tests/test_venue_parity_contract.sh` (run as Step 0j of every test leg) turns
any violation — inline harness logic in a workflow, a bypassed wrapper, a
missing `--help` — into a red build.

Every entry answers `--help` (authoritative, agent-oriented: modes, env, exit
codes) and rejects unknown flags with exit 2 + `Please consult --help.`

## The legs

| leg | entry | what a run gives you |
|---|---|---|
| **test** | `test.sh` | DEFAULT = the venue leg: static contracts (Step 0a–0r) + CLEAN ASan+UBSan build + all suites via the parallel harness + prod-binary guards. `--suites a,b` = iteration mode (incremental rebuild, subset, seconds). `--tsan` = the ThreadSanitizer leg. CLANGARM64 gets CI's trap-UBSan flags automatically. |
| **package wrappers** | `ci/test-package-wrappers.sh` | Go, npm, and PyPI runtime-set publication/lock suites on the current host. CI runs the same entry on Linux and Windows so both platform lock implementations gate. |
| **build** | `build.sh` | CLEAN production runtime set (native executable + authenticated integration asset; `--with-ui` adds one content-addressed UI pack). ccache via `env.sh` makes repeats fast; `CCACHE_COMPILERCHECK=content` guarantees a hit is byte-identical to a cold compile — never stale. `--version`, `STATIC=1`, `BUILD_DIR=`. |
| **lint** | `lint.sh` | clang-tidy + cppcheck + clang-format (+ no-skips policy). `--ci` = the CI gate set (no clang-tidy). Drives the same make targets as `make lint`/`lint-ci`. |
| **smoke (unix)** | `smoke-local.sh` | Stages a full release fixture, serves it on a kernel-assigned port, runs `smoke-test.sh` (ALL phases incl. download/install/update E2E) inside a disposable HOME/XDG/TMP sandbox. `ui` variant makes a missing verified UI pack a FAILURE. `CBM_SMOKE_ARTIFACT_DIR` = smoke an extracted release artifact verbatim (release mode). |
| **smoke (windows)** | `../test-infrastructure/vm/vm-smoke.sh` | Same verified runtime-set contract on the real Windows VM, plus the user-PATH registry guard (prepare/verify/cleanup). |
| **smoke-invariants** | `smoke-invariants.sh` | Production-path resilience battery (MCP handshake, all tools invocable, malformed-input handling, supervised crash/hang recovery) — no fixture server or install E2E. `smoke.yml` runs an explicitly seam-enabled build on the WIDEST source matrix; release artifacts remain seam-free and use the release-shaped smoke legs above. |
| **soak** | `soak-legs.sh` | The release-gating soak SEQUENCE: `quick` then `query-leak` (the #581 detector — never reindexes, so RSS growth = query-path leak), each guarded by a completion-summary check. `--legs quick` for the ASan single-leg variant. Duration is per leg. |

Internal harnesses — never called directly by a venue (the contract forbids
it): `smoke-test.sh` (phases; wrappers provide fixture server + sandbox),
`soak-test.sh` (one soak run; `soak-legs.sh` provides the sequence + guards),
`run-tests-parallel.sh` (reached through `test.sh`).

## Conventions

- **Exit codes:** 0 = pass · 2 = usage error · 90 = guard (a run died without
  its completion summary — never counts as green) · anything else = the leg's
  real failure.
- **Iteration is a flag, not a side-tool:** the fast paths (`--suites`,
  `--legs`) are modes of the SAME entry the gates run, so a dev loop can never
  drift from the venue behaviour.
- **Env sandboxing:** the smoke wrappers neutralize every agent-config
  destination override; a smoke can never scribble on your real config.

## Recommended workflows

- **Iterating on a change:** `scripts/test.sh --suites <suite>` (seconds,
  incremental, same ASan+UBSan flags as the gate). List suites:
  `build/c/test-runner --list-suites`. Debugging a Windows-on-ARM trap:
  re-run with `SANITIZE=` for a plain build, or use the emulated
  `win.sh ubsan-*` pair for full diagnostics.
- **Before any push (the 3-OS ladder):** `scripts/test.sh` (macOS, full) →
  `./test-infrastructure/run.sh full` (Linux + TSan + smoke) →
  `test-infrastructure/vm/win.sh test-par` + `guards` + `smoke-install`
  (+ `soak` when the change touches memory/daemon paths).
- **Concurrency-touching change:** add `scripts/test.sh --tsan` early — the
  same leg CI gates on.
- **Release-shaped verification:** `CBM_SMOKE_ARTIFACT_DIR=<extracted artifact>
  scripts/smoke-local.sh <binary> [ui]` smokes exactly what would ship.
- **A leg is red in CI but green locally:** first suspect environment shape,
  not code — the preflights (`win.sh` automatic; `scripts/ci/preflight-docker.sh`)
  and `test-infrastructure/README.md`'s residuals list cover the knowable
  differences.

See `scripts/ci/README.md` for the CI plumbing and
`test-infrastructure/README.md` for the venue map.
