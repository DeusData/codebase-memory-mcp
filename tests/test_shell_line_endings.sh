#!/usr/bin/env bash
# Regression guard: shell entrypoints must remain LF in Windows checkouts so
# they can run directly from WSL and MSYS without a `bash\r` shebang failure.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

failures=0
checked=0
while IFS= read -r -d '' path &&
    IFS= read -r -d '' attribute &&
    IFS= read -r -d '' eol; do
    checked=$((checked + 1))
    if [[ "$eol" != "lf" ]]; then
        echo "FAIL: $path must declare eol=lf (got ${eol:-unset})" >&2
        failures=$((failures + 1))
    fi
done < <(
    git ls-files -z '*.sh' 'scripts/git-hooks/*' 'scripts/hooks/*' |
        git check-attr -z --stdin eol
)

if (( failures > 0 )); then
    echo "FAIL: $failures shell entrypoint(s) lack an LF checkout contract" >&2
    exit 1
fi

echo "Shell line-ending contract passed ($checked files)"
