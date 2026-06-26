# codebase-memory-mcp — Developer Notes for Claude

## Build & Test (C server)

All C targets use `Makefile.cbm`:

```bash
make -f Makefile.cbm test          # build + run full test suite (ASan/UBSan)
make -f Makefile.cbm test-leak     # heap leak check (see below)
make -f Makefile.cbm test-analyze  # Clang static analyzer (requires clang, not gcc)
```

## Memory Leak Testing

**macOS** — uses Apple's `leaks --atExit` on a separate ASan-free binary:
```bash
make -f Makefile.cbm test-leak
# Report saved to build/c/leak-report.txt
# Target line: "Process NNNNN: 0 leaks for 0 total leaked bytes."
```

**Linux** — uses LSan via ASan env var on the regular test runner:
```bash
make -f Makefile.cbm test-leak
# Report saved to build/c/leak-report.txt
# Exit 0 = no leaks.
```

Why a separate binary on macOS: `leaks` cannot inspect processes that use a custom malloc (ASan replaces it). The `test-runner-nosan` target rebuilds without `-fsanitize` flags specifically for this purpose.

## Memory-Corruption Debugging (macOS)

For non-deterministic corruption (uninit reads, use-after-free, overruns) that ASan/TSan miss — used to investigate the custom-writer B1 bug. Both run the nosan binary (ASan replaces malloc, which defeats these libmalloc knobs); set `CBM_ONLY_SUITE=<suite>` to target a slow suite.

```bash
make -f Makefile.cbm test-memory    # MallocScribble=1 + MallocPreScribble=1
                                    # uninit reads -> 0xAA, use-after-free -> 0x55 (deterministic)
make -f Makefile.cbm test-gmalloc   # Guard Malloc (libgmalloc): guard page per allocation
                                    # crashes at the exact overrun/UAF with a stack trace
# Report saved to build/c/mem-report.txt
```

`test-memory` is the macOS MSan-equivalent for uninit reads (scribble makes them deterministic). `test-gmalloc` is the strictest — it crashes at the exact bad write, pinpointing the line. (No valgrind/MSan on macOS; on Linux use `-fsanitize=memory`.)

## Project Structure (C server)

Sources live under `src/`; tests under `tests/`; vendored C libs under `vendored/`.
The Go layer (`cmd/`, `internal/`) wraps the C server via CGO — see `CONTRIBUTING.md` for the Go side.
