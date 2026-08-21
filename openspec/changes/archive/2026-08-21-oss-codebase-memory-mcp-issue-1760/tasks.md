## 1. Activation recovery fix

- [ ] 1.1 In `cli_activation_production_reserve` (src/cli/cli.c), after the generation probe, add the `#ifndef _WIN32` recovery block: when `generation == 1 && cbm_daemon_ipc_lifetime_reservation_probe(endpoint) == 0 && cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup_lock) == 1`, re-run the generation probe; keep the existing `generation != 0` decision branch unchanged
- [ ] 1.2 Verify: `make -f Makefile.cbm test-focused TEST_SUITES=cli` compiles and passes (existing activation tests unchanged)

## 2. Regression tests

- [ ] 2.1 Add `TEST(cli_activation_recovers_stale_rendezvous_publication_issue1760)` in tests/test_cli.c: isolate runtime parent via test seam, fork a child that binds a Unix socket at the endpoint address, listens, signals ready and `_exit(0)` without unlink (hard-kill residue), then run the real activation guard (POSIX `install` path with `--force --skip-config --yes --dir=<tmp>`) and assert rc == 0 and the stale socket is gone
- [ ] 2.2 Add a second assertion in the same test (or a sibling): with a LIVE listener still bound and held (child stays alive), the guard returns BUSY and the socket remains published — the recovery must not disturb a live daemon
- [ ] 2.3 Verify: `make -f Makefile.cbm test-focused TEST_SUITES=cli` passes with the new tests

## 3. Final validation

- [ ] 3.1 Run `make -f Makefile.cbm test` (full suite) — passes
- [ ] 3.2 `OPENSPEC_NO_UPDATE_CHECK=1 openspec validate oss-codebase-memory-mcp-issue-1760 --json` — clean
- [ ] 3.3 Archive the change: `OPENSPEC_NO_UPDATE_CHECK=1 openspec archive oss-codebase-memory-mcp-issue-1760 --yes`
