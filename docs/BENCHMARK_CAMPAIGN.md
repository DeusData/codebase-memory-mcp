# Legacy benchmark documentation path

This document moved to [`docs/BENCHMARK_EXPERIMENTS.md`](BENCHMARK_EXPERIMENTS.md).

New documentation and interfaces use "experiment". These legacy compatibility
names remain available for existing runsets and automation:

- `scripts/run-benchmark-campaign.py` remains available as a backwards-compatible
  shim for `benchmarks/run_experiments.py`.
- `--campaign-root` still works as an alias for `--experiment-root`.
- Automatic runs retain `.worktrees/benchmark-campaign/` so old results resume.

This file is kept as a short pointer stub (rather than deleted) so existing links
and bookmarks to `docs/BENCHMARK_CAMPAIGN.md` keep resolving.
