#!/usr/bin/env python3
"""Compatibility entry point for benchmarks/fact_comparisons.py."""

try:
    from scripts._benchmark_compat import load_public
except ModuleNotFoundError:
    from _benchmark_compat import load_public


load_public(globals(), "fact_comparisons.py", "cbm_benchmark_fact_comparisons")


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821
