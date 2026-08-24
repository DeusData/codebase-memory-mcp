# Legacy benchmark documentation path

This document moved to [`docs/BENCHMARK_EXPERIMENTS.md`](BENCHMARK_EXPERIMENTS.md).

New documentation and interfaces use "experiment". Existing runsets remain readable:

- `--campaign-root` still works as an alias for `--experiment-root`.
- Automatic runs retain `.worktrees/benchmark-campaign/` so old results resume.
- Retained `scripts/benchmark-incremental-speed.py` plan entries resolve to
  `benchmarks/run_benchmark.py` when the recorded path no longer exists.

This file is kept as a short pointer stub (rather than deleted) so existing links
and bookmarks to `docs/BENCHMARK_CAMPAIGN.md` keep resolving.
