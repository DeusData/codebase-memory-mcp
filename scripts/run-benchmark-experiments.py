#!/usr/bin/env python3
"""Compatibility entry point for benchmarks/run_experiments.py."""

try:
    from scripts._benchmark_compat import load_public
except ModuleNotFoundError:
    from _benchmark_compat import load_public


load_public(globals(), "run_experiments.py", "cbm_benchmark_run_experiments")


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821
