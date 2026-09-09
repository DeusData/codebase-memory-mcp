#!/usr/bin/env python3
"""Generate small public surfaces from product-manifest.json.

The default mode writes generated files. --check exits non-zero if committed
outputs differ, allowing CI to prevent version/capability drift.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "product-manifest.json"
HEADER_PATH = ROOT / "src" / "product_manifest_generated.h"
DOC_PATH = ROOT / "docs" / "CAPABILITIES.md"
PUBLIC_CLAIM_PATHS = [
    ROOT / "README.md",
    ROOT / "server.json",
    ROOT / "docs" / "index.html",
    ROOT / "docs" / "llms.txt",
    ROOT / "pkg" / "npm" / "README.md",
    ROOT / "pkg" / "pypi" / "README.md",
    ROOT / "pkg" / "chocolatey" / "codebase-memory-mcp.nuspec",
    ROOT
    / "pkg"
    / "winget"
    / "manifests"
    / "d"
    / "DeusData"
    / "CodebaseMemoryMcp"
    / "0.8.1"
    / "DeusData.CodebaseMemoryMcp.locale.en-US.yaml",
]
VERSION_PATHS = [
    ROOT / "server.json",
    ROOT / "docs" / "index.html",
    ROOT / "pkg" / "npm" / "package.json",
    ROOT / "pkg" / "pypi" / "pyproject.toml",
    ROOT / "pkg" / "chocolatey" / "codebase-memory-mcp.nuspec",
]


def load_manifest() -> dict:
    with MANIFEST_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def render_header(manifest: dict) -> str:
    product = manifest["product"]
    toolsets = manifest["toolsets"]

    def tool_macro(name: str, items: list[str]) -> str:
        rendered = [f"#define {name}(APPLY) \\"]
        for index, item in enumerate(items):
            suffix = " \\" if index < len(items) - 1 else ""
            rendered.append(f'    APPLY("{item}"){suffix}')
        return "\n".join(rendered)

    return (
        "/* Generated from product-manifest.json. "
        "Run scripts/generate-product-surfaces.py. */\n"
        "#ifndef CBM_PRODUCT_MANIFEST_GENERATED_H\n"
        "#define CBM_PRODUCT_MANIFEST_GENERATED_H\n\n"
        f'#define CBM_PRODUCT_NAME "{product["name"]}"\n'
        f'#define CBM_PRODUCT_VERSION "{product["version"]}"\n'
        f'#define CBM_PRODUCT_STATUS "{product["status"]}"\n\n'
        f'#define CBM_PRODUCT_CORE_TOOL_COUNT {len(toolsets["core"])}\n'
        f'#define CBM_PRODUCT_ADVANCED_TOOL_COUNT {len(toolsets["advanced"])}\n'
        f'#define CBM_PRODUCT_ADMIN_TOOL_COUNT {len(toolsets["admin"])}\n\n'
        f'{tool_macro("CBM_PRODUCT_CORE_TOOLS", toolsets["core"])}\n\n'
        f'{tool_macro("CBM_PRODUCT_ADVANCED_TOOLS", toolsets["advanced"])}\n\n'
        f'{tool_macro("CBM_PRODUCT_ADMIN_TOOLS", toolsets["admin"])}\n\n'
        "#endif\n"
    )


def display_language(name: str) -> str:
    aliases = {
        "cpp": "C++",
        "csharp": "C#",
        "javascript": "JavaScript",
        "php": "PHP",
        "typescript": "TypeScript",
    }
    return aliases.get(name, name.capitalize())


def render_docs(manifest: dict) -> str:
    tiers = manifest["language_tiers"]
    tools = manifest["toolsets"]
    core_languages = ", ".join(display_language(item) for item in tiers["core"])
    preview_languages = ", ".join(display_language(item) for item in tiers["preview"])
    core_tools = ", ".join(f"`{item}`" for item in tools["core"])
    advanced_tools = ", ".join(f"`{item}`" for item in tools["advanced"])
    admin_tools = ", ".join(f"`{item}`" for item in tools["admin"])
    return f"""# Product capabilities

This file is generated from `product-manifest.json`.

`codebase-memory-mcp` is a preview local code-intelligence layer for coding
agents. Its product goals are fresh indexes, reproducible evidence and
token-efficient retrieval.

## Language quality

- Core: {core_languages}.
- Preview static resolvers: {preview_languages}.
- Structural: other vendored Tree-sitter grammars, best effort only.
- Removed: Lean and the dedicated SystemVerilog parser. `.sv` files use the
  experimental Verilog structural fallback.

## MCP toolsets

- Core: {core_tools}.
- Advanced: {advanced_tools}.
- Admin: {admin_tools}.

Advanced and administrative tools are not advertised by default.

## Evidence policy

Performance and quality claims must identify a reproducible benchmark artifact.
The historical 66-language results belong to the
[project preprint](https://arxiv.org/abs/2603.27277); they are not a blanket
guarantee for the current worktree or every language.
"""


def check_or_write(path: Path, expected: str, check: bool) -> bool:
    current = path.read_text(encoding="utf-8") if path.exists() else None
    if current == expected:
        return True
    if check:
        print(f"out of date: {path.relative_to(ROOT)}", file=sys.stderr)
        return False
    path.write_text(expected, encoding="utf-8")
    return True


def validate_public_claims(manifest: dict) -> bool:
    """Reject known-unverified claims and obsolete public terminology."""
    forbidden = [
        *manifest["forbidden_unverified_claims"],
        "Hybrid LSP",
    ]
    valid = True
    for path in PUBLIC_CLAIM_PATHS:
        if not path.exists():
            continue
        content = path.read_text(encoding="utf-8")
        for claim in forbidden:
            if claim.casefold() in content.casefold():
                print(
                    f"forbidden public claim in {path.relative_to(ROOT)}: {claim}",
                    file=sys.stderr,
                )
                valid = False
    return valid


def validate_versions(manifest: dict) -> bool:
    """Keep release-facing metadata aligned with the product manifest."""
    expected = manifest["product"]["version"]
    version_pattern = re.compile(r"\b\d+\.\d+\.\d+\b")
    valid = True
    for path in VERSION_PATHS:
        if not path.exists():
            continue
        versions = set(version_pattern.findall(path.read_text(encoding="utf-8")))
        if versions and versions != {expected}:
            rendered = ", ".join(sorted(versions))
            print(
                f"version drift in {path.relative_to(ROOT)}: {rendered}; "
                f"expected {expected}",
                file=sys.stderr,
            )
            valid = False
    return valid


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    manifest = load_manifest()
    results = [
        check_or_write(HEADER_PATH, render_header(manifest), args.check),
        check_or_write(DOC_PATH, render_docs(manifest), args.check),
        validate_public_claims(manifest),
        validate_versions(manifest),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
