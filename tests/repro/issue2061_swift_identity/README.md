# Swift overload reproduction (#2061)

Ordinary regressions use the repository entry point:

```sh
scripts/test.sh --suites extraction,callable_sig,registry,pipeline,index_format
```

`main.c` checks the original three-file issue fixture through the production
pipeline/store (10 assertions). `mcp_driver.c` checks five real JSON-RPC envelopes,
signature-bearing presentation rows, counts, caller paths and the exact text tree.
These standalone drivers are **opt-in**, not automatically executed by CI.
A graph pass does not prove MCP output.

From the repository root on a Unix host with CONTRIBUTING.md prerequisites:

```sh
make -f tests/repro/issue2061_swift_identity/repro.mk issue2061-repro
evidence=$(mktemp -d)
build/c/issue2061-driver graph "$evidence/graph"
mkdir "$evidence/cache"
test ! -e "$evidence/graph/graph.db-wal"
cp "$evidence/graph/graph.db" "$evidence/cache/issue2061-swift-identity.db"
CBM_CACHE_DIR="$evidence/cache" CBM_ALLOWED_ROOT="$evidence/graph/repo" \
  build/c/issue2061-driver mcp issue2061-swift-identity
```

The graph driver creates its fresh child directory and closes its DB handles.
Copy only after its successful exit and with no active writer/WAL. Retain outputs
and actual exits. Default flags include ASan/UBSan; do not suppress leaks.
Exit 0 means observations passed, 1 an assertion mismatch, 2 a setup/API error.
Sanitizers can make a semantically successful run fail.

See [validation and limitations](../../../docs/SWIFT_IDENTITY_VALIDATION.md).
[Historical evidence](https://github.com/DavidHLP/codebase-memory-mcp/tree/39d5db48a83f94641fccbb44c8cbc0233b5e2a51/tests/repro/issue2061_swift_identity/evidence/2026-10-05)
remains public at an immutable commit, including failures. Old private paths,
approvals and host launchers are not current reproduction steps.
