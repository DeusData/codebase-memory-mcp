---
phase: 05-web-ui-launch-repair
verified: 2026-04-13T00:51:11Z
status: passed
score: 5/5 must-haves verified
---

# Phase 5: Web UI Launch Repair Verification Report

**Phase Goal:** Maintainers can launch the web UI with `--ui=true` and get the intended embedded UI startup path instead of a broken or ignored enable flag.
**Verified:** 2026-04-13T00:51:11Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Running the normal web UI entrypoint with `--ui=true` starts the embedded UI instead of leaving it down. | ✓ VERIFIED | `src/main.c` loads runtime UI config into one `ui_cfg`, gates startup with `if (ui_cfg.ui_enabled && CBM_EMBEDDED_FILE_COUNT > 0)`, and the behavioral regression passed: `PASS: persisted UI launch contract works`. |
| 2 | A later launch with no `--ui=` flag still starts the UI when `--ui=true` was previously persisted. | ✓ VERIFIED | `load_runtime_ui_config()` in `src/main.c` loads persisted config before startup, and `scripts/test_ui_launch_persisted_flag.py` relaunches with only `--port=<port>` and waits for HTTP reachability. |
| 3 | Running `--ui=false` persists the disabled state and keeps a later no-flag launch from starting the UI. | ✓ VERIFIED | `parse_ui_flags()` writes `ui_enabled = false` for non-`true` `--ui=` values, `cbm_ui_config_save()` persists it, and the regression checks both saved JSON and closed-port behavior after a later no-flag relaunch. |
| 4 | The `--ui=true` and `--ui=false` persisted contract remains explicit in code and help text. | ✓ VERIFIED | `print_help()` in `src/main.c` still prints the exact persisted help lines; direct `./build/c/codebase-memory-mcp --help` output matched them; regression also asserts both lines. |
| 5 | Regression coverage protects the `--ui=true` startup path from breaking again. | ✓ VERIFIED | `scripts/test_ui_launch_persisted_flag.py` covers enable, persisted relaunch, disable, and help text; `Makefile.cbm` `test:` target includes `python3 -B scripts/test_ui_launch_persisted_flag.py`. |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `src/main.c` | Single-source-of-truth CLI parsing, persistence, and UI startup gating | ✓ VERIFIED | Exists, substantive, and wired. `load_runtime_ui_config()` performs `cbm_ui_config_load(&ui_cfg)`, `parse_ui_flags(argc, argv, &ui_cfg)`, optional `cbm_ui_config_save(&ui_cfg)`, then startup uses the same `ui_cfg` for `cbm_http_server_new(ui_cfg.ui_port)`. |
| `scripts/test_ui_launch_persisted_flag.py` | End-to-end persisted UI launch regression | ✓ VERIFIED | Exists, substantive, and wired. Builds `cbm-with-ui`, launches `build/c/codebase-memory-mcp`, checks localhost reachability, verifies persisted JSON keys, tests relaunch/disable, and validates help output. |
| `Makefile.cbm` | Default test entrypoint runs the regression | ✓ VERIFIED | Exists, substantive, and wired. `test:` target includes `python3 -B scripts/test_ui_launch_persisted_flag.py`. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `src/main.c` | `src/ui/config.c` | `cbm_ui_config_load/cbm_ui_config_save` | ✓ WIRED | `gsd-tools verify key-links` found the configured pattern in source. |
| `src/main.c` | `src/ui/http_server.h` | `cbm_http_server_new(ui_cfg.ui_port)` | ✓ WIRED | Startup gate calls `cbm_http_server_new(ui_cfg.ui_port)` only when enabled and assets exist. |
| `scripts/test_ui_launch_persisted_flag.py` | `build/c/codebase-memory-mcp` | subprocess launches with isolated HOME/cache | ✓ WIRED | `BUILD_BIN` points at the built binary and `launch()` starts it under isolated `HOME`. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `src/main.c` | `ui_cfg.ui_enabled`, `ui_cfg.ui_port` | `cbm_ui_config_load()` in `src/ui/config.c` + `parse_ui_flags()` overrides + optional `cbm_ui_config_save()` | Yes | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Persisted UI launch contract works end-to-end | `python3 -B scripts/test_ui_launch_persisted_flag.py` | `PASS: persisted UI launch contract works` | ✓ PASS |
| Help output still advertises the persisted contract | `./build/c/codebase-memory-mcp --help` | Output includes `--ui=true    Enable HTTP graph visualization (persisted)` and `--ui=false   Disable HTTP graph visualization (persisted)` | ✓ PASS |
| Default test entrypoint contains the regression | static read of `Makefile.cbm` `test:` target | `python3 -B scripts/test_ui_launch_persisted_flag.py` is present on line 425 | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `UI-01` | `05-01-PLAN.md` | Maintainer can launch the web UI with `--ui=true` and get the intended embedded UI startup path without launch failure | ✓ SATISFIED | Entry-point flow repaired in `src/main.c`; regression passed; help contract preserved; `REQUIREMENTS.md` Phase 5 mapping matches the plan. |

No orphaned Phase 5 requirements were found in `REQUIREMENTS.md`.

### Anti-Patterns Found

No actionable anti-patterns found in `src/main.c`, `scripts/test_ui_launch_persisted_flag.py`, or `Makefile.cbm`. A `nullPointer...` grep hit in `Makefile.cbm` was a cppcheck suppression string, not a stub or incomplete implementation.

### Gaps Summary

No gaps found. The repaired startup path exists in code, is wired to persisted config load/save and HTTP server startup, preserves the documented persisted CLI contract, and is protected by a working end-to-end regression.

---

_Verified: 2026-04-13T00:51:11Z_
_Verifier: OpenCode (gsd-verifier)_
