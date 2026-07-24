"""Shared loader for historical benchmark script entry points."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


def load_public(namespace: dict[str, Any], filename: str, module_name: str) -> None:
    implementation = Path(__file__).resolve().parents[1] / "benchmarks" / filename
    spec = importlib.util.spec_from_file_location(module_name, implementation)
    if not spec or not spec.loader:
        raise RuntimeError(f"cannot load benchmark implementation: {implementation}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    namespace["_benchmark_implementation"] = module
    namespace.update(
        {
            name: getattr(module, name)
            for name in dir(module)
            if not name.startswith("__")
        }
    )
