# Bounded two-pass indexing

This is the batching-only follow-up to #1925. `CBM_STREAMING_BATCH_FILES=128`
opts into two-pass extraction when discovery finds more than 128 files. Valid
values are 1–4096; an invalid value fails the extraction phase. With the variable
unset, the existing path is used. The option works with one or multiple workers.

The bound is on live **full extraction results**, not total process memory.
Pass A registers definitions and keeps a compact definition/import surface.
Once the whole registry, namespace map and import graph are available, it builds
cross-file LSP definitions and registries. Pass B re-extracts one batch, resolves
its relationships and releases it. The global graph and compact surface still
scale with repository size; one huge file can still be expensive. No automatic
threshold, global scheduler, admission budget or platform RSS policy is added.

Java participates in the shared cross registry. `CBM_DISABLE_LSP_CROSS` retains
its existing presence-based semantics in both paths (including a value of `0`).
ObjectScript macros and package manifests are collected for the whole repository.
Pass B reuses definition nodes, deduplicates parse/skip diagnostics, and serial
Go implementation/override scans run once after all batches. The final manifest
and publication guards remain responsible for detecting source changes during a run.

The TSNodeStack changes from #1925 are excluded: this branch retains main's
#2013 scratch-lifetime implementation. Result-array allocation is untouched.
The worker scoping and complexity-order changes belong to #2076 and #2079;
there is no `compare_node_qn` rewrite in this branch.

## Measurements

Native macOS 26.6.2 arm64, 24 GiB RAM, Apple clang 21.0.0, production `-O2`,
`CBM_WORKERS=4`, three fresh runs per row. Baseline is upstream
`aa44c28ea5ea82a5f811f0bace4f7857a68cac80`. The generated TypeScript corpus has
2,049 files / 1,893,350 source bytes. Repomix is a `git archive` of
`e3b15a406ed78d8a463620a032a059ce911bfc0e`, without dependencies or git history.

**Whole-worker peak RSS**, measured by the parent's `wait4().ru_maxrss` at exit,
and wall seconds (including worker startup, executable verification and publication):

- Generated TypeScript, baseline: 648.4 / 648.9 / 648.8 MiB; 2.691 / 2.638 / 2.644 s.
- Same source, feature off: 650.2 / 650.0 / 648.0 MiB; 2.651 / 2.669 / 2.663 s.
- Same source, batch 128: 300.8 / 300.9 / 301.0 MiB; 3.227 / 3.231 / 3.242 s.
- Repomix, baseline: 275.4 / 271.0 / 272.3 MiB; 2.055 / 2.082 / 2.053 s.
- Repomix, batch 128: 197.5 / 193.6 / 194.4 MiB; 2.405 / 2.453 / 2.417 s.

The generated corpus's median process peak falls about 54%, at about 22% more
worker wall time. Repomix's median peak falls about 29%, at about 18% more wall
time. These are corpus-specific measurements, not an RSS guarantee. The
`mem.phase` figures are lower, since they only sample selected phases; they are
recorded separately and are not reported as the whole-worker peak.

Across all 15 measured worker runs, each corpus's normalized **persisted nodes,
node properties, edges including properties, and LSP surface JSON** match its
baseline exactly. IDs are replaced with qualified names; JSON object key order
is normalized. The TypeScript database contains 22,533 nodes / 57,348 edges;
Repomix contains 12,051 nodes / 19,154 edges. Repomix's parse_partial_count stays
4; the synthetic count stays 0. SQLite integrity checks return `ok`. Persisted
counts are taken from SQLite, not inferred from the earlier CLI response counts.

Machine-readable measurements and equality results are in
[bounded-two-pass-results.json](bounded-two-pass-results.json). Raw logs, response
JSON and database digests are produced by the reproduction script.

## Reproduce

Build the baseline and this branch in separate worktrees with `scripts/build.sh`.
Keep the corpus at the same absolute path for all runs: project identity and
qualified names incorporate that path. `BASE_BINARY` and `FEATURE_BINARY` below
must point to the respective production executables. `RUNS` is a new output
directory; `RUNTIME_PARENT` is a short private directory owned by your account
(short enough for Unix socket paths). Every run creates its own cache and runtime.

```sh
python3 scripts/benchmark-streaming.py --binary "$BASE_BINARY" --repo "$CORPUS" \
  --output "$RUNS/baseline" --runtime-parent "$RUNTIME_PARENT" --runs 3
python3 scripts/benchmark-streaming.py --binary "$FEATURE_BINARY" --repo "$CORPUS" \
  --output "$RUNS/feature-off" --runtime-parent "$RUNTIME_PARENT" --runs 3
python3 scripts/benchmark-streaming.py --binary "$FEATURE_BINARY" --repo "$CORPUS" \
  --output "$RUNS/batch-128" --runtime-parent "$RUNTIME_PARENT" --runs 3 --batch 128
```

The harness invokes the fingerprint-validated internal worker directly, so the
measured child is the indexing worker rather than a CLI waiting on a daemon.
It requires Python 3 and `os.wait4` (macOS/Linux). This measurement harness is
not evidence of a native Windows run; Windows coverage comes from the C tests
in the project's platform CI. No CI result is asserted by this document.

Generate the synthetic corpus in an empty directory:

```python
from pathlib import Path
r = Path("corpus")
r.mkdir()
(r / "shared.ts").write_text("export function helper(x: number): number { return x + 1; }\n")
for i in range(2048):
    methods = "\n".join(
        f"  method{j}(x: number): number {{ let sum = 0; for (let k = 0; k < x; k++) {{ sum += helper(k); }} return sum; }}"
        for j in range(8)
    )
    (r / f"unit{i:04d}.ts").write_text(
        f"import {{ helper }} from './shared';\nexport class Unit{i} {{\n{methods}\n}}\n"
    )
```

The C pipeline tests compare batch sizes 1 and 7 (one and four workers), Java
cross-file calls, Python/TypeScript inheritance across batches, disabled cross-LSP,
full graph relationship/property sets, persisted surfaces and partial-parse
counts. A separate lifetime test frees the original Go/Python/Java/Rust extraction
before rebuilding the compact surface under ASan/UBSan.
