---
name: security-audit
description: Run the full 8-layer security audit and analyze results
---

Run the 8-layer security audit:

```bash
make -f Makefile.cbm security
```

Analyze the output. The 8 layers are:

1. **Static allow-list audit** — Checks for dangerous calls (`system`, `popen`, `fork`, network) not in `scripts/security-allowlist.txt`
2. **Binary string scan** — Searches compiled binary for suspicious strings
3. **UI audit** — Validates embedded frontend assets
4. **Install audit** — Checks install scripts for unsafe operations
5. **Network egress test** — Verifies no unauthorized network access
6. **MCP robustness (fuzz)** — Sends malformed JSON-RPC to test input handling
7. **Vendored dependency integrity** — Verifies vendored source checksums
8. **Frontend integrity** — Checks graph-ui build artifacts

For each failure, explain what the layer checks, why it failed, and how to fix it. If a new dangerous call is intentional, guide adding it to `scripts/security-allowlist.txt`.
