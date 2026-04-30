#!/usr/bin/env bash
set -euo pipefail

# audit-grammar-security.sh — Scan vendored tree-sitter grammar scanner.c
# files for dangerous patterns that could indicate malicious code.
#
# Grammars are third-party C source compiled into our binary. parser.c files
# are auto-generated (low risk), but scanner.c files are hand-written C with
# access to memory allocation and input text.
#
# Usage: scripts/audit-grammar-security.sh [directory]
#   Default: internal/cbm/vendored/grammars/

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GRAMMAR_DIR="${1:-$ROOT/internal/cbm/vendored/grammars}"

if [ ! -d "$GRAMMAR_DIR" ]; then
    echo "ERROR: directory not found: $GRAMMAR_DIR" >&2
    exit 1
fi

echo "=== Grammar Security Audit ==="
echo "Scanning: $GRAMMAR_DIR"
echo ""

TOTAL=0
WARNED=0
BLOCKED=0

# Safe includes that scanners legitimately use
SAFE_INCLUDES='string\.h|stdint\.h|stdbool\.h|stdlib\.h|wctype\.h|stdio\.h|stddef\.h|limits\.h|assert\.h|ctype\.h|wchar\.h|math\.h|stdalign\.h|stdarg\.h|float\.h|inttypes\.h'

# Dangerous includes
DANGER_INCLUDES='unistd\.h|sys/|netdb\.h|dlfcn\.h|signal\.h|spawn\.h|pthread\.h|fcntl\.h|dirent\.h|termios\.h|arpa/|netinet/'

# Dangerous function calls (word-boundary \b to avoid matching lex_accept, etc.)
# getenv("TREE_SITTER_DEBUG") is a standard tree-sitter pattern, excluded.
DANGER_CALLS='\bsystem\s*\(|\bexec[lvpe]+\s*\(|\bpopen\s*\(|\bfopen\s*\(|\bfwrite\s*\(|\bsocket\s*\(|\bfork\s*\(|\bdlopen\s*\(|\bconnect\s*\(|\bbind\s*\(|\blisten\s*\(|\bsendto\s*\(|\brecvfrom\s*\(|\bmmap\s*\(|\bmprotect\s*\('
# getenv is only dangerous for non-debug uses
DANGER_GETENV='\bgetenv\s*\('
SAFE_GETENV='TREE_SITTER_DEBUG'

# Suspicious patterns
SUSPICIOUS='__attribute__\s*\(\s*\(\s*constructor|__attribute__\s*\(\s*\(\s*destructor|asm\s*\(|__asm__|__asm\b'

for grammar_dir in "$GRAMMAR_DIR"/*/; do
    name=$(basename "$grammar_dir")
    TOTAL=$((TOTAL + 1))
    issues=""

    for src in "$grammar_dir"scanner.c "$grammar_dir"scanner.cc "$grammar_dir"*.h "$grammar_dir"*.inc; do
        [ -f "$src" ] || continue
        basename_src=$(basename "$src")

        # Skip tree_sitter/ subdirectory headers (those are standard)
        case "$src" in
            */tree_sitter/*) continue ;;
        esac

        # Check dangerous includes
        while IFS= read -r line; do
            issues="${issues}  WARN  $basename_src: dangerous include: $line\n"
        done < <(grep -nE "#include\s*<($DANGER_INCLUDES)" "$src" 2>/dev/null || true)

        # Check dangerous calls
        while IFS= read -r line; do
            issues="${issues}  BLOCK $basename_src: dangerous call: $line\n"
        done < <(grep -nE "$DANGER_CALLS" "$src" 2>/dev/null || true)

        # Check getenv separately (allow TREE_SITTER_DEBUG)
        while IFS= read -r line; do
            if ! echo "$line" | grep -qF "$SAFE_GETENV"; then
                issues="${issues}  BLOCK $basename_src: dangerous call: $line\n"
            fi
        done < <(grep -nE "$DANGER_GETENV" "$src" 2>/dev/null || true)

        # Check suspicious patterns
        while IFS= read -r line; do
            issues="${issues}  BLOCK $basename_src: suspicious pattern: $line\n"
        done < <(grep -nE "$SUSPICIOUS" "$src" 2>/dev/null || true)

        # Check for base64-like long encoded strings (40+ alphanumeric chars)
        while IFS= read -r line; do
            issues="${issues}  WARN  $basename_src: possible encoded data: $line\n"
        done < <(grep -nE '"[A-Za-z0-9+/]{60,}={0,2}"' "$src" 2>/dev/null || true)
    done

    if [ -n "$issues" ]; then
        if echo -e "$issues" | grep -q "BLOCK"; then
            echo "BLOCK $name:"
            echo -e "$issues"
            BLOCKED=$((BLOCKED + 1))
        else
            echo "WARN  $name:"
            echo -e "$issues"
            WARNED=$((WARNED + 1))
        fi
    fi
done

echo "────────────────────────────────────────────"
echo "  Scanned: $TOTAL grammars"
echo "  Clean:   $((TOTAL - WARNED - BLOCKED))"
echo "  Warned:  $WARNED"
echo "  Blocked: $BLOCKED"
echo "────────────────────────────────────────────"

if [ "$BLOCKED" -gt 0 ]; then
    echo ""
    echo "FAILED: $BLOCKED grammar(s) have dangerous patterns. Review before vendoring."
    exit 1
fi

echo ""
echo "PASSED: No dangerous patterns found."
exit 0
