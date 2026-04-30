#!/usr/bin/env bash
set -euo pipefail

# Layer 8: Vendored dependency integrity — verifies vendored C sources match
# checked-in checksums. Detects supply chain tampering of vendored libraries.
#
# Libraries covered: mimalloc, mongoose, sqlite3, tre, xxhash, yyjson
#
# Usage: scripts/security-vendored.sh

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHECKSUMS="$ROOT/scripts/vendored-checksums.txt"

if [[ ! -f "$CHECKSUMS" ]]; then
    echo "FAIL: checksums file not found: $CHECKSUMS"
    exit 1
fi

echo "=== Layer 8: Vendored Dependency Integrity ==="

# Detect shasum command (shasum on macOS, sha256sum on Linux)
if command -v sha256sum &>/dev/null; then
    SHA_CMD="sha256sum"
elif command -v shasum &>/dev/null; then
    SHA_CMD="shasum -a 256"
else
    echo "SKIP: no sha256sum or shasum available"
    exit 0
fi

FAIL=0
CHECKED=0
MISSING=0

# Verify each file in the checksums list
while IFS=' ' read -r expected_hash filepath; do
    # Skip empty lines
    [[ -z "$expected_hash" ]] && continue

    # Strip the two-space separator from filepath (sha256sum format: "hash  file")
    filepath="${filepath#"${filepath%%[![:space:]]*}"}"

    full_path="$ROOT/$filepath"
    if [[ ! -f "$full_path" ]]; then
        echo "MISSING: $filepath"
        MISSING=$((MISSING + 1))
        continue
    fi

    actual_hash=$($SHA_CMD "$full_path" | cut -d' ' -f1)
    CHECKED=$((CHECKED + 1))

    if [[ "$actual_hash" != "$expected_hash" ]]; then
        echo "MISMATCH: $filepath"
        echo "  expected: $expected_hash"
        echo "  actual:   $actual_hash"
        FAIL=1
    fi
done < "$CHECKSUMS"

# Verify every vendored library directory has checksum coverage.
# If someone adds a new vendored library, this forces them to register it.
echo ""
echo "--- Checking vendored library coverage ---"
while IFS= read -r libdir; do
    libname=$(basename "$libdir")
    # Check if any file from this library is in the checksums
    if ! grep -q "vendored/${libname}/" "$CHECKSUMS" 2>/dev/null; then
        echo "BLOCKED: vendored/${libname}/ has NO checksum coverage"
        echo "  Run: scripts/security-vendored.sh --update"
        FAIL=1
    fi
done < <(find "$ROOT/vendored" -mindepth 1 -maxdepth 1 -type d | sort)

# Also check for unexpected NEW files in vendored/ that aren't in the checksums
UNEXPECTED=0
while IFS= read -r file; do
    relpath="${file#"$ROOT/"}"
    if ! grep -q "$relpath" "$CHECKSUMS" 2>/dev/null; then
        echo "NEW FILE: $relpath (not in checksums — run 'scripts/security-vendored.sh --update' to add)"
        UNEXPECTED=$((UNEXPECTED + 1))
    fi
done < <(find "$ROOT/vendored" -type f \( -name '*.c' -o -name '*.h' \) | sort)

echo ""
echo "Checked: $CHECKED files"
[[ $MISSING -gt 0 ]] && echo "Missing: $MISSING files"
[[ $UNEXPECTED -gt 0 ]] && echo "New (untracked): $UNEXPECTED files"

# Handle --update flag: regenerate checksums
# ── Dangerous call scan: vendored code must not contain subprocess calls ───

echo ""
echo "--- Scanning vendored code for dangerous calls ---"

# Subprocess spawning: must not exist in ANY vendored library
SUBPROCESS_FUNCS='[^a-z_]system\(|[^a-z]popen\(|[^a-z_]execl\(|[^a-z_]execv\(|[^a-z_]fork\('
if grep -rn -E "$SUBPROCESS_FUNCS" "$ROOT/vendored/" --include='*.c' --include='*.h' 2>/dev/null \
    | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' | grep -v 'typedef' \
    | grep -v 'indicating that a fork' > /dev/null 2>&1; then
    echo "BLOCKED: Subprocess calls found in vendored code:"
    grep -rn -E "$SUBPROCESS_FUNCS" "$ROOT/vendored/" --include='*.c' --include='*.h' 2>/dev/null \
        | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' | grep -v 'typedef' \
        | grep -v 'indicating that a fork' | head -10
    FAIL=1
else
    echo "OK: No subprocess calls (system/popen/exec/fork) in vendored code"
fi

# Network calls: only allowed in mongoose (HTTP library)
NETWORK_FUNCS='[^a-z_]connect\(|[^a-z_]socket\(|[^a-z_]sendto\(|[^a-z_]bind\('
NON_MONGOOSE_NETWORK=$(grep -rn -E "$NETWORK_FUNCS" "$ROOT/vendored/" --include='*.c' --include='*.h' 2>/dev/null \
    | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' | grep -v 'typedef' \
    | grep -v 'mongoose' | grep -v 'sqlite3.*bind()' || true)
if [[ -n "$NON_MONGOOSE_NETWORK" ]]; then
    echo "BLOCKED: Network calls found outside mongoose:"
    echo "$NON_MONGOOSE_NETWORK" | head -10
    FAIL=1
else
    echo "OK: Network calls only in mongoose (expected)"
fi

# dlopen/LoadLibrary: only allowed in sqlite3 (extension loading) and mimalloc (Windows APIs)
DYNLOAD_FUNCS='dlopen\(|LoadLibrary\('
NON_SQLITE_DYNLOAD=$(grep -rn -E "$DYNLOAD_FUNCS" "$ROOT/vendored/" --include='*.c' --include='*.h' 2>/dev/null \
    | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' \
    | grep -v 'sqlite3' | grep -v 'mimalloc' || true)
if [[ -n "$NON_SQLITE_DYNLOAD" ]]; then
    echo "BLOCKED: Dynamic library loading found outside sqlite3:"
    echo "$NON_SQLITE_DYNLOAD" | head -10
    FAIL=1
else
    echo "OK: dlopen/LoadLibrary only in sqlite3 (blocked by authorizer at runtime)"
fi

# Verify the dangerous call rules cover every vendored library.
# Known safe: yyjson, xxhash, tre (pure computation, no OS interaction)
# Known with exceptions: mongoose (network), sqlite3 (dlopen), mimalloc (LoadLibrary)
# If a new library appears, the scan above already checks it — but this ensures
# we've consciously evaluated each library.
KNOWN_VENDORED="mimalloc mongoose nomic sqlite3 tre xxhash yyjson"
while IFS= read -r libdir; do
    libname=$(basename "$libdir")
    found=false
    for known in $KNOWN_VENDORED; do
        if [[ "$libname" == "$known" ]]; then
            found=true
            break
        fi
    done
    if ! $found; then
        echo "BLOCKED: vendored/${libname}/ is not in the known vendored library list"
        echo "  Evaluate it for dangerous calls, then add to KNOWN_VENDORED in this script."
        FAIL=1
    fi
done < <(find "$ROOT/vendored" -mindepth 1 -maxdepth 1 -type d | sort)

# Also scan tree-sitter grammars (internal/cbm/vendored/) — 650MB, 20M lines.
# Use fast fixed-string grep (-F) for each pattern to avoid slow regex on huge codebase.
if [[ -d "$ROOT/internal/cbm/vendored" ]]; then
    GRAMMAR_FAIL=false
    for pattern in 'system(' 'popen(' 'execl(' 'execv(' 'fork(' 'connect(' 'socket(' 'sendto(' 'dlopen(' 'LoadLibrary('; do
        HITS=$(grep -rl -F "$pattern" "$ROOT/internal/cbm/vendored/" --include='*.c' --include='*.h' 2>/dev/null | head -3 || true)
        if [[ -n "$HITS" ]]; then
            echo "BLOCKED: '$pattern' found in vendored grammars:"
            echo "$HITS" | sed 's|.*/vendored/|  vendored/|'
            GRAMMAR_FAIL=true
        fi
    done
    if $GRAMMAR_FAIL; then
        FAIL=1
    else
        echo "OK: No dangerous calls in vendored tree-sitter grammars"
    fi
fi

if [[ "${1:-}" == "--update" ]]; then
    echo ""
    echo "Updating checksums..."
    find "$ROOT/vendored" -type f \( -name '*.c' -o -name '*.h' \) | sort | while IFS= read -r f; do
        $SHA_CMD "$f"
    done > "$CHECKSUMS"
    echo "Updated: $CHECKSUMS ($(wc -l < "$CHECKSUMS" | tr -d ' ') files)"
    exit 0
fi

if [[ $FAIL -ne 0 ]]; then
    echo ""
    echo "=== VENDORED INTEGRITY CHECK FAILED ==="
    echo "A vendored file has been modified. If this is intentional (upgrade),"
    echo "run: scripts/security-vendored.sh --update"
    exit 1
fi

if [[ $UNEXPECTED -gt 0 ]]; then
    echo ""
    echo "WARNING: New vendored files not in checksums. Run --update if intentional."
fi

echo ""
echo "=== Vendored integrity check passed ==="
