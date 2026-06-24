#!/bin/bash
# vendor-grammar.sh: Vendor a single tree-sitter grammar into internal/cbm/vendored/grammars/<name>/
# Usage: ./scripts/vendor-grammar.sh <repo_url> <name> [subdir]
#   repo_url: GitHub repository URL (e.g., https://github.com/tree-sitter/tree-sitter-json)
#   name:     Target directory name (e.g., json)
#   subdir:   Optional subdirectory within repo containing src/ (e.g., "fsharp" for monorepo grammars)

set -euo pipefail

REPO_URL="$1"
NAME="$2"
SUBDIR="${3:-}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
GRAMMAR_DIR="$PROJECT_DIR/internal/cbm/vendored/grammars/$NAME"
TMPDIR="$(mktemp -d)"

trap 'rm -rf "$TMPDIR"' EXIT

echo "Vendoring $NAME from $REPO_URL..."

git clone --depth 1 "$REPO_URL" "$TMPDIR/repo" 2>/dev/null

SRC_DIR="$TMPDIR/repo/src"
if [ -n "$SUBDIR" ]; then
    SRC_DIR="$TMPDIR/repo/$SUBDIR/src"
fi

if [ ! -f "$SRC_DIR/parser.c" ]; then
    echo "ERROR: $SRC_DIR/parser.c not found" >&2
    exit 1
fi

mkdir -p "$GRAMMAR_DIR/tree_sitter"

cp "$SRC_DIR/parser.c" "$GRAMMAR_DIR/"

if [ -f "$SRC_DIR/scanner.c" ]; then
    cp "$SRC_DIR/scanner.c" "$GRAMMAR_DIR/"
fi
if [ -f "$SRC_DIR/scanner.cc" ]; then
    echo "WARNING: $NAME has C++ scanner (scanner.cc) — needs special handling" >&2
fi

# Copy tree_sitter headers
if [ -d "$SRC_DIR/tree_sitter" ]; then
    cp "$SRC_DIR/tree_sitter/"*.h "$GRAMMAR_DIR/tree_sitter/" 2>/dev/null || true
fi

# Copy any extra headers (.h, .inc files) used by scanners
# Examples: tag.h (Vue/Svelte/Astro), unicode.h (PureScript/Typst),
# TokenTree.h/.inc (VHDL)
for f in "$SRC_DIR"/*.h "$SRC_DIR"/*.inc; do
    [ -f "$f" ] && cp "$f" "$GRAMMAR_DIR/"
done
# Copy common/ subdirectory if present (e.g., F# scanner uses common/scanner.h)
if [ -d "$SRC_DIR/common" ]; then
    cp -r "$SRC_DIR/common" "$GRAMMAR_DIR/"
fi

# Copy LICENSE file from upstream repo
REPO_ROOT="$TMPDIR/repo"
if [ -n "$SUBDIR" ]; then
    # For monorepos, check subdir first, then repo root
    if [ -f "$REPO_ROOT/$SUBDIR/LICENSE" ]; then
        cp "$REPO_ROOT/$SUBDIR/LICENSE" "$GRAMMAR_DIR/LICENSE"
    elif [ -f "$REPO_ROOT/LICENSE" ]; then
        cp "$REPO_ROOT/LICENSE" "$GRAMMAR_DIR/LICENSE"
    elif [ -f "$REPO_ROOT/LICENSE.md" ]; then
        cp "$REPO_ROOT/LICENSE.md" "$GRAMMAR_DIR/LICENSE"
    elif [ -f "$REPO_ROOT/COPYING" ]; then
        cp "$REPO_ROOT/COPYING" "$GRAMMAR_DIR/LICENSE"
    else
        echo "WARNING: No LICENSE file found for $NAME" >&2
    fi
else
    if [ -f "$REPO_ROOT/LICENSE" ]; then
        cp "$REPO_ROOT/LICENSE" "$GRAMMAR_DIR/LICENSE"
    elif [ -f "$REPO_ROOT/LICENSE.md" ]; then
        cp "$REPO_ROOT/LICENSE.md" "$GRAMMAR_DIR/LICENSE"
    elif [ -f "$REPO_ROOT/COPYING" ]; then
        cp "$REPO_ROOT/COPYING" "$GRAMMAR_DIR/LICENSE"
    else
        echo "WARNING: No LICENSE file found for $NAME" >&2
    fi
fi

echo "Vendored $NAME to $GRAMMAR_DIR"
ls -la "$GRAMMAR_DIR/"
