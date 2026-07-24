#!/usr/bin/env python3
"""Backwards-compatible shim for benchmarks/run_experiments.py.

This filename loads and re-exports `benchmarks/run_experiments.py` so existing
invocations keep working. New code uses the canonical filename and experiment
terminology. The legacy root flags and `.worktrees/benchmark-campaign/` location
remain accepted for retained automation and results.
"""

from __future__ import annotations

try:
    from scripts._benchmark_compat import load_public
except ModuleNotFoundError:
    from _benchmark_compat import load_public


load_public(globals(), "run_experiments.py", "cbm_benchmark_run_experiments_legacy")


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821 - re-exported from _impl above
