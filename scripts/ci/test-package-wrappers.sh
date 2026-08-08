#!/usr/bin/env bash
# Gate the three package-manager wrappers against the host's real filesystem
# and process-lock semantics. CI runs this once on Unix and once on Windows.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    cat <<'EOF'
Usage: scripts/ci/test-package-wrappers.sh

Run the Go, npm, and PyPI wrapper suites on the current host. The caller must
provide the Go version declared by pkg/go/go.mod, Node.js >=18, and Python >=3.8.
The Go toolchain is kept local so this test never hides a missing provisioned
toolchain behind an implicit download.
EOF
}

case "${1:-}" in
-h | --help)
    usage
    exit 0
    ;;
"") ;;
*)
    echo "test-package-wrappers.sh: unexpected argument '$1'. Please consult --help." >&2
    exit 2
    ;;
esac

for tool in go npm; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "test-package-wrappers.sh: required tool is missing: $tool" >&2
        exit 2
    fi
done

PYTHON=()
if command -v python3 >/dev/null 2>&1; then
    PYTHON=(python3)
elif command -v python >/dev/null 2>&1; then
    PYTHON=(python)
elif command -v py >/dev/null 2>&1; then
    PYTHON=(py -3)
else
    echo "test-package-wrappers.sh: Python 3 is required" >&2
    exit 2
fi
if ! "${PYTHON[@]}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 8) else 1)'; then
    echo "test-package-wrappers.sh: Python >=3.8 is required" >&2
    exit 2
fi

echo "=== Package wrapper: Go ==="
(
    cd "$ROOT/pkg/go"
    GOTOOLCHAIN=local go test ./...
)

echo "=== Package wrapper: npm ==="
(
    cd "$ROOT/pkg/npm"
    npm test
)

echo "=== Package wrapper: PyPI ==="
(
    cd "$ROOT/pkg/pypi"
    "${PYTHON[@]}" -m unittest discover -s tests -p 'test_*.py'
)

echo "=== All package wrapper suites passed ==="
