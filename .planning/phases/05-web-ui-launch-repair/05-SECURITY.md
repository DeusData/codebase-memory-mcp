---
phase: 05
slug: web-ui-launch-repair
status: verified
threats_open: 0
asvs_level: 1
created: 2026-04-12
---

# Phase 05 - Security

> Per-phase security contract: threat register, accepted risks, and audit trail.

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|---------------|
| CLI args / persisted config -> `src/main.c` launch logic | Untrusted local input controls whether the embedded HTTP UI is enabled and which port it uses. | Local CLI flag values and persisted `ui_enabled` / `ui_port` configuration |
| `src/main.c` -> `src/ui/http_server.c` | The entrypoint decides whether a localhost listener is created for the embedded UI. | Listener enablement decision and localhost port value |

---

## Threat Register

| Threat ID | Category | Component | Disposition | Mitigation | Status |
|-----------|----------|-----------|-------------|------------|--------|
| T-05-01 | T | `parse_ui_flags()` in `src/main.c` | mitigate | `src/main.c:234-258` keeps `--ui=` on the literal `"true"` check only and persists only `ui_enabled` / `ui_port`; `scripts/test_ui_launch_persisted_flag.py:56-81,151-176,189-193` verifies the exact persisted keys plus the supported `--ui=true` / `--ui=false` contract. | closed |
| T-05-02 | D | persisted UI startup path | mitigate | `src/main.c:252-258,295,344-351` routes load, parse, save, and startup through the same `ui_cfg` instance; `scripts/test_ui_launch_persisted_flag.py:120-197` exercises enable, persisted relaunch, and disable against the built binary; `python3 -B scripts/test_ui_launch_persisted_flag.py` and `rtk make -f Makefile.cbm test` both passed during this audit. | closed |
| T-05-03 | I | HTTP UI bind behavior | mitigate | `src/main.c:344-351` still starts the server with `cbm_http_server_new(ui_cfg.ui_port)` and preserves the `ui.no_assets` warning path; `src/ui/http_server.c:1143-1147` binds to `http://127.0.0.1:%d` only; `scripts/test_ui_launch_persisted_flag.py:24-53` verifies reachability and closure on `127.0.0.1`. | closed |

*Status: open · closed*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party)*

---

## Accepted Risks Log

No accepted risks.

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-04-12 | 3 | 3 | 0 | OpenCode |

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter

**Approval:** verified 2026-04-12
