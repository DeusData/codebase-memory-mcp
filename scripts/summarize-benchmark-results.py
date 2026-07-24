#!/usr/bin/env python3
"""Compatibility entry point for benchmarks/summarize_results.py."""

try:
    from scripts._benchmark_compat import load_public
except ModuleNotFoundError:
    from _benchmark_compat import load_public


load_public(globals(), "summarize_results.py", "cbm_benchmark_summarize_results")


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821
