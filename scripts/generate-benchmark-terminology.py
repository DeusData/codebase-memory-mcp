#!/usr/bin/env python3
"""Compatibility entry point for benchmarks/generate_terminology.py."""

try:
    from scripts._benchmark_compat import load_public
except ModuleNotFoundError:
    from _benchmark_compat import load_public


load_public(globals(), "generate_terminology.py", "cbm_benchmark_generate_terminology")


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821
