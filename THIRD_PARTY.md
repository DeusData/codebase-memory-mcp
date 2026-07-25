# Third-Party Licenses

This project vendors third-party code. We are grateful to the authors and
maintainers of these projects for making their work freely available.
Every vendored component directory carries the upstream `LICENSE`
(or `COPYING` / `NOTICE`) file alongside the sources.

## Tree-sitter Runtime

The tree-sitter C runtime is vendored in `internal/cbm/vendored/ts_runtime/`.

- **Project:** [tree-sitter](https://github.com/tree-sitter/tree-sitter)
- **License:** MIT
- **Copyright:** (c) 2018–2024 Max Brunsfeld

**Local modification** (`internal/cbm/vendored/ts_runtime/src/stack.c`, #913): a
single CBM patch bounds the recursive ambiguity-merge in `stack_node_add_link`
at `CBM_TS_STACK_MERGE_MAX_DEPTH` (512). Deeply nested grammar-ambiguous input
(e.g. Perl `f(f(f(...)))`) otherwise recurses once per level on the native C
stack and overflows it during parsing (SIGSEGV on the ~1 MB Windows thread
stack, and even the 8 MB POSIX stack at extreme depth) before any extractor
runs. Past the cap the ambiguity is left on the GLR stack instead of merged —
still a valid parse, never a wrong one — mirroring the existing
`MAX_LINK_COUNT` bail-out. The change is clearly marked `// CBM patch:` inline.
**On re-vendor (e.g. ts_runtime → 0.26.x): re-apply this bound.**

The shared scanner helpers in `internal/cbm/vendored/common/` (`scanner.h`,
`tag.h`) originate from
[tree-sitter-html](https://github.com/tree-sitter/tree-sitter-html) (MIT,
(c) 2014 Max Brunsfeld) and carry that project's `LICENSE` in
`internal/cbm/vendored/common/`.

The core runtime headers in `internal/cbm/vendored/common/tree_sitter/`
(`alloc.h`, `array.h`, `parser.h`) are part of the tree-sitter C runtime
([tree-sitter](https://github.com/tree-sitter/tree-sitter), MIT,
(c) 2018 Max Brunsfeld) and carry their own `LICENSE` in that directory.

## Tree-sitter Grammars

159 pre-generated parsers are vendored in `internal/cbm/vendored/grammars/<lang>/`
(generated `parser.c` plus `scanner.c` where applicable, compiled statically).
Each grammar is the work of its upstream authors and each grammar directory
contains the upstream `LICENSE` file.

The **canonical provenance record** — upstream repository, pinned commit, and
cross-registry verification status for every grammar — is
[`internal/cbm/vendored/grammars/MANIFEST.md`](internal/cbm/vendored/grammars/MANIFEST.md).

License summary:

- Nearly all grammars are **MIT**-licensed.
- `clojure` ([sogaiu/tree-sitter-clojure](https://github.com/sogaiu/tree-sitter-clojure)) is **CC0-1.0**;
  `fennel` is **CC0-1.0**; `jinja2` and `just` are **Apache-2.0**;
  `pine` is **ISC** (declared by its upstream).
- The grammars authored in-house for this project (`cobol`, `form`, `janet`,
  `magma`, `protobuf`, `wolfram`) are **MIT** under the project's own license,
  (c) DeusData. Six further grammars (`assembly`, `cfml`, `cfscript`,
  `dotenv`, `pine`, `qml`) are self-maintained forks that retain their
  original upstream authors' licenses — see the manifest for per-grammar
  provenance.

### tree-sitter-objectscript (UDL + routine)

- **Project:** [intersystems/tree-sitter-objectscript](https://github.com/intersystems/tree-sitter-objectscript)
- **License:** MIT
- **Copyright:** (c) 2025 InterSystems Corporation
- **Vendored at:** `internal/cbm/vendored/grammars/objectscript_udl/`, `internal/cbm/vendored/grammars/objectscript_routine/`
- **Pinned commit:** `a7ffcdf`
- **Notes:** InterSystems-maintained grammar for the ObjectScript language (InterSystems IRIS / Caché). Vendor-maintained; not in nvim-treesitter or Helix registries. Each `scanner.c`'s upstream `#include "../../common/scanner.h"` is repointed to a per-directory `objectscript_common.h` (verbatim copy of upstream `common/scanner.h`).

## Vendored C/C++ Libraries

| Library | Path | License | Project |
|---------|------|---------|---------|
| SQLite 3 | `vendored/sqlite3/` | Public Domain | [sqlite.org](https://www.sqlite.org/) |
| mimalloc | `vendored/mimalloc/` | MIT | [microsoft/mimalloc](https://github.com/microsoft/mimalloc) |
| yyjson | `vendored/yyjson/` | MIT | [ibireme/yyjson](https://github.com/ibireme/yyjson) |
| xxHash | `vendored/xxhash/` | BSD-2-Clause | [Cyan4973/xxHash](https://github.com/Cyan4973/xxHash) |
| TRE | `vendored/tre/` | BSD-2-Clause | [laurikari/tre](https://github.com/laurikari/tre) |
| LZ4 | `internal/cbm/vendored/lz4/` | BSD-2-Clause (library files) | [lz4/lz4](https://github.com/lz4/lz4) |
| Zstandard | `internal/cbm/vendored/zstd/` | BSD-3-Clause (dual BSD / GPLv2 — BSD selected) | [facebook/zstd](https://github.com/facebook/zstd) |
| simplecpp | `internal/cbm/vendored/simplecpp/` | 0BSD | [danmar/simplecpp](https://github.com/danmar/simplecpp) |
| Verstable | `internal/cbm/vendored/verstable/` | MIT | [JacksonAllan/Verstable](https://github.com/JacksonAllan/Verstable) |
| wyhash | `internal/cbm/vendored/wyhash/` | Unlicense (public domain) | [wangyi-fudan/wyhash](https://github.com/wangyi-fudan/wyhash) |

Local modifications to these libraries are documented next to the
vendored sources (currently only SQLite: `vendored/sqlite3/PATCHES.md`,
raising the Unix VFS `MAX_PATHNAME` ceiling from 512 to 4096 to match
CBM's 4 KiB path support). Patches must be reapplied on every upstream
refresh and are covered by `scripts/vendored-checksums.txt`.

The graph-UI HTTP server is a first-party implementation
(`src/ui/httpd.c` + `src/ui/http_server.c`) — no third-party HTTP library
is used.

## Embedded Model Data

Semantic vector search uses static token embeddings derived from the
**nomic-embed-code** model, vendored in `vendored/nomic/`:

- **Model:** [nomic-ai/nomic-embed-code](https://huggingface.co/nomic-ai/nomic-embed-code)
- **License:** Apache License 2.0
- **Copyright:** (c) Nomic AI

See `vendored/nomic/NOTICE` for the exact derivation procedure
(per-token inference + int8 quantization via `scripts/extract_nomic_vectors.py`).

## Hybrid LSP — Reference Language Servers

The Hybrid LSP layer (`internal/cbm/lsp/`) is an original C implementation
written for this project. **It contains no source code from any language
server.** Its type-resolution behavior is structurally inspired by, and
validated for output compatibility against, the published behavior of the
following language servers and language specifications. They are listed here
as acknowledgment; their licenses are noted for reference:

| Language | Reference implementation / specification | Upstream license |
|----------|-------------------------------------------|------------------|
| TypeScript / JavaScript | tsserver ([microsoft/TypeScript](https://github.com/microsoft/TypeScript)), [typescript-go](https://github.com/microsoft/typescript-go) | Apache-2.0 |
| Python | [pyright](https://github.com/microsoft/pyright) | MIT |
| Go | gopls ([golang/tools](https://github.com/golang/tools)) | BSD-3-Clause |
| PHP | PHP language reference + Composer PSR-4 autoloading specification | — |
| C# | Roslyn ([dotnet/roslyn](https://github.com/dotnet/roslyn)) | MIT |
| C / C++ | clangd ([llvm/llvm-project](https://github.com/llvm/llvm-project)) | Apache-2.0 WITH LLVM-exception |
| Java | Java Language Specification; output parity with [Eclipse JDT LS](https://github.com/eclipse-jdtls/eclipse.jdt.ls) | EPL-2.0 (reference only) |
| Kotlin | Kotlin language specification; [fwcd/kotlin-language-server](https://github.com/fwcd/kotlin-language-server) | MIT |
| Rust | [rust-analyzer](https://github.com/rust-lang/rust-analyzer) | MIT OR Apache-2.0 |

### Standard-library type data

The stdlib type registries in `internal/cbm/lsp/generated/` were produced as
follows:

- **Python** (`python_stdlib_data.c`) — generated from
  [python/typeshed](https://github.com/python/typeshed) type stubs
  (commit `a7912d521e16ff63caf7a8b64b9072542be36777`), **Apache-2.0**,
  (c) the typeshed contributors. The generator is `scripts/gen-py-stdlib.py`.
- **Go** (`go_stdlib_data.c`) — generated by introspecting the public API of
  the Go standard library ([golang/go](https://github.com/golang/go),
  BSD-3-Clause).
- **Java, Kotlin, C#, PHP, C/C++, Rust** — hand-curated from public API
  documentation and language specifications; no upstream source code was
  extracted or transcribed.

## Embedded Graph UI

Release binaries built with `--with-ui` embed the compiled `graph-ui/`
frontend bundle. Its npm dependencies (React, three.js, @react-three/*,
radix-ui, lucide-react, tailwindcss, and friends) are all under permissive
licenses (MIT / ISC / Apache-2.0 / Zlib); the exact set is recorded in
`graph-ui/package.json` and `graph-ui/package-lock.json`, and the per-package
license texts of the production bundle are appended to the
`THIRD_PARTY_NOTICES.md` shipped inside the `-ui` release archives
(generated by `scripts/gen-ui-licenses.py`).
