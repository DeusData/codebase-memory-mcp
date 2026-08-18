#!/usr/bin/env bash
# Regression guard: shell entrypoints must remain LF in Windows checkouts so
# they can run directly from WSL and MSYS without a `bash\r` shebang failure.
#
# Distilled from PR #1272 by @xumian520, who both hit the breakage under
# core.autocrlf=true and wrote this contract so it stays fixed. The .sh rule
# itself landed via #1314; the extensionless git hooks need their own
# entries, which this guard also covers.

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

if ((checked == 0)); then
    echo "FAIL: the line-ending contract matched no files — the glob set is broken" >&2
    exit 1
fi

if ((failures > 0)); then
    echo "FAIL: $failures shell entrypoint(s) lack an LF checkout contract" >&2
    exit 1
fi

echo "Shell line-ending contract passed ($checked files)"
