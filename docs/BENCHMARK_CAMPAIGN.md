# Reproducible benchmark campaigns

This document moved to [`docs/BENCHMARK_EXPERIMENTS.md`](BENCHMARK_EXPERIMENTS.md).

"Campaign" and "experiment" refer to the same thing in this project; the
terminology moved to "experiment" while keeping every path, flag, and filename
that existing runsets and automation depend on:

- `scripts/run-benchmark-campaign.py` still works unchanged as a backwards-compatible
  shim for `scripts/run-benchmark-experiments.py`.
- `--campaign-root` still works as an alias for `--experiment-root`.
- The retained-results directory `.worktrees/benchmark-campaign/` keeps its name.

This file is kept as a short pointer stub (rather than deleted) so existing links
and bookmarks to `docs/BENCHMARK_CAMPAIGN.md` keep resolving.
