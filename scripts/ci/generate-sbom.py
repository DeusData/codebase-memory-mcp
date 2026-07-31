#!/usr/bin/env python3
"""Generate the release SPDX SBOM (sbom.json).

Canonical release step (called by release.yml). Lived inline in workflow YAML
until 2026-07-26 — vendored-dependency versions belong in a reviewable,
venue-independent script, and the venue-parity contract forbids logic in
workflow run-blocks.

Usage: scripts/ci/generate-sbom.py <version>   (e.g. v0.9.1)

Keep versionInfo entries in sync with the vendored trees; the authoritative
per-grammar license list is internal/cbm/vendored/grammars/MANIFEST.md.
"""
from __future__ import annotations

import datetime
import glob
import json
import os
import sys

if len(sys.argv) != 2 or sys.argv[1] in {"-h", "--help"}:
    print(__doc__.strip(), file=sys.stderr)
    sys.exit(0 if len(sys.argv) == 2 else 2)

version = sys.argv[1]
n_grammars = len(
    [d for d in glob.glob("internal/cbm/vendored/grammars/*") if os.path.isdir(d)]
)
sbom = {
    "spdxVersion": "SPDX-2.3",
    "dataLicense": "CC0-1.0",
    "SPDXID": "SPDXRef-DOCUMENT",
    "name": f"codebase-memory-mcp-{version}",
    "documentNamespace": f"https://github.com/DeusData/codebase-memory-mcp/releases/{version}",
    "creationInfo": {
        "created": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "creators": ["Tool: codebase-memory-mcp-release-pipeline"],
    },
    "packages": [
        {"SPDXID": "SPDXRef-Package-sqlite3", "name": "sqlite3", "versionInfo": "3.51.3", "licenseDeclared": "blessing", "downloadLocation": "https://sqlite.org", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-yyjson", "name": "yyjson", "versionInfo": "0.12.0", "licenseDeclared": "MIT", "downloadLocation": "https://github.com/ibireme/yyjson", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-mimalloc", "name": "mimalloc", "versionInfo": "3.3.2", "licenseDeclared": "MIT", "downloadLocation": "https://github.com/microsoft/mimalloc", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-xxhash", "name": "xxhash", "versionInfo": "0.8.3", "licenseDeclared": "BSD-2-Clause", "downloadLocation": "https://github.com/Cyan4973/xxHash", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-tre", "name": "tre", "versionInfo": "0.8.0", "licenseDeclared": "BSD-2-Clause", "downloadLocation": "https://github.com/laurikari/tre", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-tree-sitter", "name": "tree-sitter", "versionInfo": "0.24.4", "licenseDeclared": "MIT", "downloadLocation": "https://github.com/tree-sitter/tree-sitter", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-lz4", "name": "lz4", "versionInfo": "1.10.0", "licenseDeclared": "BSD-2-Clause", "downloadLocation": "https://github.com/lz4/lz4", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-zstd", "name": "zstd", "versionInfo": "1.5.7", "licenseDeclared": "BSD-3-Clause", "downloadLocation": "https://github.com/facebook/zstd", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-simplecpp", "name": "simplecpp", "versionInfo": "1.x", "licenseDeclared": "0BSD", "downloadLocation": "https://github.com/danmar/simplecpp", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-verstable", "name": "verstable", "versionInfo": "2.2.1", "licenseDeclared": "MIT", "downloadLocation": "https://github.com/JacksonAllan/Verstable", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-wyhash", "name": "wyhash", "versionInfo": "final-4.3", "licenseDeclared": "Unlicense", "downloadLocation": "https://github.com/wangyi-fudan/wyhash", "filesAnalyzed": False},
        {"SPDXID": "SPDXRef-Package-nomic-embed-code", "name": "nomic-embed-code-token-embeddings", "versionInfo": "1.0", "licenseDeclared": "Apache-2.0", "downloadLocation": "https://huggingface.co/nomic-ai/nomic-embed-code", "filesAnalyzed": False, "comment": "Derived int8 token embeddings; see vendored/nomic/NOTICE"},
        {"SPDXID": "SPDXRef-Package-tree-sitter-grammars", "name": "tree-sitter-grammars-aggregate", "versionInfo": f"{n_grammars}-grammars", "licenseDeclared": "MIT", "downloadLocation": "NOASSERTION", "filesAnalyzed": False, "comment": f"Aggregate of {n_grammars} vendored tree-sitter grammars, compiled statically. Predominantly MIT; non-MIT families present include CC0-1.0 (clojure, fennel), Apache-2.0 (elixir, erlang, gleam, hcl, ini, jinja2, just, pkl, sway, wit), ISC (pine, templ), Unlicense (fish); first-party grammars (c) DeusData, MIT. This summary is non-exhaustive; the authoritative complete per-grammar license list is internal/cbm/vendored/grammars/MANIFEST.md, and full license texts ship in THIRD_PARTY_NOTICES.md inside each archive."},
    ],
}
json.dump(sbom, open("sbom.json", "w"), indent=2)
print(f"sbom.json written ({n_grammars} grammars, version {version})")
