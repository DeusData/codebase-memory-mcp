#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

case "$*" in
-h | --help)
    cat <<'EOF'
Usage: bash scripts/ci/lint-workflows.sh

Validate all GitHub Actions workflows with actionlint 1.7.12 on PATH.
ShellCheck and Pyflakes are separate checks, not enabled by this entry point.
EOF
    exit 0
    ;;
"") ;;
*)
    echo "lint-workflows.sh: unexpected arguments '$*'. Please consult --help." >&2
    exit 2
    ;;
esac

cd "$ROOT"
exec actionlint -shellcheck= -pyflakes=