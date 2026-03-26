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

## Project Structure (C server)

Sources live under `src/`; tests under `tests/`; vendored C libs under `vendored/`.
The Go layer (`cmd/`, `internal/`) wraps the C server via CGO — see `CONTRIBUTING.md` for the Go side.
