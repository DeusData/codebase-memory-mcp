#!/usr/bin/env python3
"""Backwards-compatible shim for scripts/run-benchmark-experiments.py.

This filename loads and re-exports `run-benchmark-experiments.py` so existing
invocations keep working. New code uses the canonical filename and experiment
terminology. The legacy root flags and `.worktrees/benchmark-campaign/` location
remain accepted for retained automation and results.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_IMPL_PATH = Path(__file__).resolve().with_name("run-benchmark-experiments.py")
_SPEC = importlib.util.spec_from_file_location("run_benchmark_experiments", _IMPL_PATH)
assert _SPEC and _SPEC.loader
_impl = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_impl)

# Re-export every public name from the canonical implementation so this shim is a
# drop-in replacement for the module that used to be defined directly in this file.
globals().update(
    {_name: getattr(_impl, _name) for _name in dir(_impl) if not _name.startswith("__")}
)


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821 - re-exported from _impl above
