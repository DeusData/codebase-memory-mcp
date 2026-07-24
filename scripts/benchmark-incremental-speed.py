#!/usr/bin/env python3
"""Compatibility entry point for benchmarks/incremental_speed.py."""

try:
    from scripts._benchmark_compat import load_public
except ModuleNotFoundError:
    from _benchmark_compat import load_public


load_public(globals(), "incremental_speed.py", "cbm_benchmark_incremental_speed")


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821
