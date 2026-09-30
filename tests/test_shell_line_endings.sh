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

# This contract reads REPOSITORY metadata (.gitattributes, through git's own
# attribute matcher). The Linux container leg mounts the WORKING TREE only, and
# a git worktree's .git is a file pointing at the host's main repository, which
# is not inside the container — so `git ls-files` cannot run there at all
# (verified 2026-09-17: `fatal: not a git repository`). The property is
# platform-independent and stays gated in every venue that has the metadata:
# the macOS host leg, the Windows VM (a real checkout at C:\cbm — 117 files
# checked there), and every hosted-CI checkout.
# Tried and rejected: mounting the host .git into the container — the worktree
# gitdir path would have to be reproduced inside, and container-root writes to
# the host repository are worse than a scoped skip; and matching .gitattributes
# patterns ourselves, which is a partial copy of git's attribute semantics and
# so a false-green risk. tests/test_version_metadata_contract.sh guards the
# same way for the same reason.
if ! git rev-parse --git-dir >/dev/null 2>&1; then
    echo "SKIP: no repository metadata in this checkout (working tree without" \
        "its git dir) — the line-ending contract gates on the host leg and CI"
    exit 0
fi

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
