#!/usr/bin/env python3
"""Generate the benchmark config-key allowlist from the C sources.

Why this exists
---------------
`cbm_config_value_is_valid` (src/cli/cli.c) accepts any key absent from
CBM_CONFIG_REGISTRY so that extension/private keys survive:

    return true; /* preserve extension/private keys not owned by this registry */

A mistyped or removed key is therefore written to the config DB, exits 0, and is
recorded in a benchmark cell's `parameters.config_overrides` as if it had taken
effect. The run then reports a difference of exactly zero with no warning, which
is indistinguishable from "this knob does not matter". Every tuning campaign
built on that is invalid.

The benchmark harness cannot rely on the product to reject typos, so it validates
against this generated allowlist instead (see apply_config_overrides in
run_benchmark.py). Generating rather than hand-maintaining keeps the list honest:
regenerate after any config change and the diff shows exactly what moved.

Two key populations are emitted, because they differ in how the product treats
them:

  registry  -- listed in CBM_CONFIG_REGISTRY; `config set` range-validates these.
  consumed  -- read through cbm_config_get_*() but absent from the registry, so
               `config set` accepts any value without range validation. These are
               real keys; rejecting them would break valid experiments.

Usage:
    python3 benchmarks/generate_config_keys.py            # write config-keys-v1.json
    python3 benchmarks/generate_config_keys.py --check    # verify it is up to date
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = Path(__file__).resolve().with_name("config-keys-v1.json")
SCHEMA_VERSION = 1

# Literals that are macro *values* (enum members, sentinels) rather than key
# names. They match the key-shaped pattern but are never valid `config set` keys.
NON_KEY_LITERALS = frozenset(
    {
        "after_publish",
        "always",
        "always_rehash",
        "at_publish",
        "cached_exact",
        "classic",
        "fast_mode_indexes_only",
        "full_rebuild",
        "manual",
        "never",
        "off",
        "small_deltas",
        "streamlined",
        "true",
        "false",
    }
)

KEY_SHAPE = re.compile(r"[a-z][a-z0-9_\-]*\Z")
DEFINE_RE = re.compile(r'^#define\s+(CBM_CONFIG_[A-Z0-9_]+)\s+"([^"]*)"', re.M)
REGISTRY_ENTRY_RE = re.compile(r"\{\s*(CBM_CONFIG_[A-Z0-9_]+)\s*,")
REGISTRY_DECL = "const cbm_config_entry_t CBM_CONFIG_REGISTRY[] = {"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def macro_table(root: Path) -> dict[str, str]:
    """Map every CBM_CONFIG_* macro to its string literal, across all headers."""
    table: dict[str, str] = {}
    for header in sorted(root.glob("src/**/*.h")):
        for match in DEFINE_RE.finditer(read(header)):
            table.setdefault(match.group(1), match.group(2))
    if not table:
        raise SystemExit("no CBM_CONFIG_* macros found; wrong repository root?")
    return table


def registry_macros(root: Path) -> list[str]:
    """First field of every CBM_CONFIG_REGISTRY entry, in declaration order."""
    source = read(root / "src" / "cli" / "cli.c")
    try:
        start = source.index(REGISTRY_DECL)
    except ValueError as exc:  # pragma: no cover - structural change in cli.c
        raise SystemExit(f"cannot locate {REGISTRY_DECL!r} in cli.c") from exc
    end = source.index("\n};", start)
    return REGISTRY_ENTRY_RE.findall(source[start:end])


def consumed_macros(root: Path) -> set[str]:
    """Macros passed to a cbm_config_get_*() call anywhere in the C sources."""
    call = re.compile(r"cbm_config_get(?:_[a-z]+)?\s*\(\s*[^,]+,\s*(CBM_CONFIG_[A-Z0-9_]+)")
    found: set[str] = set()
    for source in sorted(root.glob("src/**/*.c")):
        found.update(call.findall(read(source)))
    return found


def git_revision(root: Path) -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    revision = proc.stdout.strip()
    return revision if len(revision) == 40 else "unknown"


def build_document(root: Path) -> dict[str, Any]:
    macros = macro_table(root)
    registry_order = registry_macros(root)

    unresolved = sorted({m for m in registry_order if m not in macros})
    if unresolved:
        raise SystemExit(f"registry macros with no string literal: {unresolved}")

    registry = sorted({macros[m] for m in registry_order})
    registry_set = set(registry)

    consumed = sorted(
        {
            macros[m]
            for m in consumed_macros(root)
            if m in macros
            and macros[m] not in registry_set
            and macros[m] not in NON_KEY_LITERALS
            and KEY_SHAPE.match(macros[m])
        }
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_by": "benchmarks/generate_config_keys.py",
        "source_revision": git_revision(root),
        "note": (
            "Allowlist for benchmark --config overrides. cbm_config_value_is_valid "
            "in src/cli/cli.c returns true for keys absent from CBM_CONFIG_REGISTRY, "
            "so the product cannot reject a typo; the harness rejects it instead."
        ),
        "registry_note": "Listed in CBM_CONFIG_REGISTRY; `config set` range-validates these.",
        "consumed_note": (
            "Read via cbm_config_get_*() but absent from CBM_CONFIG_REGISTRY, so "
            "`config set` accepts any value unvalidated. Real keys; still allowed."
        ),
        "registry_count": len(registry),
        "consumed_count": len(consumed),
        "registry": registry,
        "consumed_unregistered": consumed,
        "keys": sorted(registry_set | set(consumed)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the checked-in file differs from freshly generated output.",
    )
    args = parser.parse_args()

    document = build_document(ROOT)
    rendered = json.dumps(document, indent=2, sort_keys=False) + "\n"

    if args.check:
        if not OUT_PATH.is_file():
            print(f"missing {OUT_PATH}", file=sys.stderr)
            return 1
        current = OUT_PATH.read_text(encoding="utf-8")
        # source_revision moves with every commit; compare the key sets only.
        old = json.loads(current)
        if old.get("keys") != document["keys"]:
            added = sorted(set(document["keys"]) - set(old.get("keys", [])))
            removed = sorted(set(old.get("keys", [])) - set(document["keys"]))
            print(f"config keys changed: added={added} removed={removed}", file=sys.stderr)
            return 1
        print(f"up to date: {len(document['keys'])} keys")
        return 0

    OUT_PATH.write_text(rendered, encoding="utf-8")
    print(
        f"wrote {OUT_PATH.name}: {document['registry_count']} registry "
        f"+ {document['consumed_count']} consumed-unregistered "
        f"= {len(document['keys'])} keys"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
