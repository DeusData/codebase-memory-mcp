# Benchmarks

There is no current release-grade benchmark artifact for the agents-first
preview. Consequently, the project does not make blanket indexing-speed,
language-count, accuracy or token-reduction guarantees.

Historical results belong to the
[project preprint](https://arxiv.org/abs/2603.27277). They describe that study's
66-language setup and must not be presented as measurements of the current
worktree.

## Publishing a result

A benchmark result is eligible for public use only when it:

1. follows [EVALUATION_PLAN.md](EVALUATION_PLAN.md);
2. pins repositories, commits, dirty patches, prompts and tool versions;
3. records hardware and all raw MCP/client traces;
4. includes warmup policy, repetition count, median, p95 and MAD;
5. includes failures and timeouts;
6. carries a normalized graph digest and checksums;
7. is committed under `benchmarks/runs/<run-id>/`.

Marketing text may cite only the metric, scope and exact artifact that supports
the claim. Removing or changing that artifact invalidates the claim.

## Local baseline

Build the binary, then run the reproducible harness against a pinned checkout:

```bash
make -f Makefile.cbm cbm
python3 scripts/benchmark-baseline.py \
  --binary build/c/codebase-memory-mcp \
  --repo /absolute/path/to/pinned/repository \
  --output benchmarks/runs/<run-id> \
  --mode structural \
  --workers 1,2,4,auto \
  --warmup 1 \
  --repetitions 5
```

The command exits non-zero when the normalized structural digests differ
between measured runs. Its output includes raw stdout/stderr, environment,
median, p95, MAD, peak RSS, database size and checksums. Record incremental
parse, resolve, persist and derived timings separately; the current harness
measures full indexing only. A developer's one-off terminal observation is
diagnostic evidence, not a publishable result.
