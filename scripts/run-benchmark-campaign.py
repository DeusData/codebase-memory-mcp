#!/usr/bin/env python3
"""Backwards-compatible shim for scripts/run-benchmark-experiments.py.

This filename is kept so existing invocations, automation, and retained runsets
under `.worktrees/benchmark-campaign/` keep working unchanged. All behavior lives
in `run-benchmark-experiments.py`, the canonical entry point; this module loads it
by path and re-exports every public name so callers that import this file's
internals directly (tests/test_benchmark_campaign.py, scripts/autotune.py's
load_campaign_runner(), scripts/summarize-benchmark-results.py's
load_campaign_runner()) keep working without modification.

New scripts and documentation should reference `run-benchmark-experiments.py`
instead; the two names are otherwise identical, including CLI flags, output, and
error text. `--experiment-root` is accepted as an alias for `--campaign-root`, and
`--allow-temporary-experiment-root` for `--allow-temporary-campaign-root`, on both
entry points.
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
globals().update({_name: getattr(_impl, _name) for _name in dir(_impl) if not _name.startswith("__")})


if __name__ == "__main__":
    raise SystemExit(main())  # noqa: F821 - re-exported from _impl above
