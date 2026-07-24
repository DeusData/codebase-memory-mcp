#!/usr/bin/env python3
"""Compatibility entry point for benchmarks/autotune.py."""

try:
    from scripts._benchmark_compat import load_public
except ModuleNotFoundError:
    from _benchmark_compat import load_public


load_public(globals(), "autotune.py", "cbm_benchmark_autotune")


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821
