#!/usr/bin/env bash
# The Windows VM sync manifest must represent Git-visible files exactly once,
# without walking through tracked symlinks.
# Source-contract patterns intentionally retain shell variables literally.
# shellcheck disable=SC2016

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/cbm-vm-manifest-test.XXXXXX")"
manifest="$(mktemp "${TMPDIR:-/tmp}/cbm-vm-manifest-output.XXXXXX")"
trap 'rm -rf -- "$FIXTURE"; rm -f -- "$manifest"' EXIT

git -C "$FIXTURE" init -q
mkdir -p "$FIXTURE/target"
printf 'tracked\n' >"$FIXTURE/target/tracked.txt"
printf 'deleted\n' >"$FIXTURE/deleted.txt"
ln -s target "$FIXTURE/Link"
git -C "$FIXTURE" add Link target/tracked.txt deleted.txt
rm "$FIXTURE/deleted.txt"
printf 'untracked\n' >"$FIXTURE/new.txt"

# shellcheck source=test-infrastructure/vm/ssh-common.sh
source "$ROOT/test-infrastructure/vm/ssh-common.sh"
cbm_vm_write_untracked_manifest "$FIXTURE" "$manifest"

saw_untracked=false
while IFS= read -r -d '' relative; do
    case "$relative" in
        Link)
            echo "FAIL: manifest duplicated a tracked symlink" >&2
            exit 1
            ;;
        Link/*)
            echo "FAIL: manifest traversed tracked symlink: $relative" >&2
            exit 1
            ;;
        target/tracked.txt)
            echo "FAIL: manifest duplicated a tracked file" >&2
            exit 1
            ;;
        new.txt) saw_untracked=true ;;
        deleted.txt)
            echo "FAIL: manifest retained deleted tracked file" >&2
            exit 1
            ;;
    esac
done <"$manifest"

$saw_untracked || { echo "FAIL: ordinary untracked file missing" >&2; exit 1; }

driver="$ROOT/test-infrastructure/vm/win.sh"
provisioner="$ROOT/test-infrastructure/vm/provision-windows.sh"
sync_block="$(sed -n '/^sync)/,/^    ;;/p' "$driver")"
if ! grep -Fq 'exec "$0" build' <<<"$sync_block"; then
    echo "FAIL: Windows VM sync must rebuild before returning" >&2
    exit 1
fi
if ! grep -Fq -- '-c \"set -e -o pipefail; $*\"' "$driver"; then
    echo "FAIL: Windows VM remote shell must propagate pipeline failures" >&2
    exit 1
fi
if ! grep -Fq 'tests/test_vm_worktree_manifest.sh' "$ROOT/scripts/test.sh"; then
    echo "FAIL: Windows VM sync contract is not wired into the test suite" >&2
    exit 1
fi
for entrypoint in "$driver" "$provisioner"; do
    if ! grep -Fq 'git -C "$ROOT" branch --show-current' "$entrypoint"; then
        echo "FAIL: Windows VM driver must default to the current local branch: $entrypoint" >&2
        exit 1
    fi
    if ! grep -Fq 'cbm_vm_require_safe_branch "$BRANCH"' "$entrypoint"; then
        echo "FAIL: Windows VM driver must validate branch text before remote use: $entrypoint" >&2
        exit 1
    fi
    if ! grep -Fq -- '-c \"set -e -o pipefail; $*\"' "$entrypoint"; then
        echo "FAIL: Windows VM driver must propagate remote pipeline failures: $entrypoint" >&2
        exit 1
    fi
done
if ! grep -Fq "JOBS='\$(nproc)'" "$provisioner" ||
    ! grep -Fq 'make -j${JOBS}' "$provisioner"; then
    echo "FAIL: Windows VM provisioner must expand the remote core count exactly once" >&2
    exit 1
fi
guards_block="$(sed -n '/^guards)/,/^    ;;/p' "$driver")"
# The guards leg builds the CI-shaped product payload (embedded UI + launcher)
# into an isolated BUILD_DIR, then hands those artifacts to the maintained
# PowerShell driver through a plain cmd shell (CI's environment shape, not
# the MSYS2 login shell whose TMP ancestry the daemon correctly refuses).
if ! grep -Fq 'scripts/build.sh --with-ui CC=clang CXX=clang++ SANITIZE= BUILD_DIR=build/guards' \
    <<<"$guards_block" ||
    ! grep -Fq 'powershell -NoProfile -ExecutionPolicy Bypass -File scripts\\test-windows.ps1' \
    <<<"$guards_block" ||
    ! grep -Fq -- '-GuardsOnly -Binary build\\guards\\codebase-memory-mcp.exe' <<<"$guards_block" ||
    ! grep -Fq -- '-Launcher build\\guards\\codebase-memory-mcp-launcher.exe' <<<"$guards_block"; then
    echo "FAIL: Windows VM guards must delegate to the maintained native-Windows driver" >&2
    exit 1
fi
if grep -Fq 'for g in tests/windows/test_*.py' <<<"$guards_block"; then
    echo "FAIL: Windows VM guards must not duplicate the native-Windows driver loop" >&2
    exit 1
fi
if [ "$(grep -c 'git clean -fdx' "$driver")" -lt 2 ]; then
    echo "FAIL: Windows VM update and sync must clean the remote worktree" >&2
    exit 1
fi
if ! grep -Fq 'git clean -fdx' "$provisioner"; then
    echo "FAIL: Windows VM reprovision update must clean the remote worktree" >&2
    exit 1
fi
if grep -Fq -- '-e build' "$driver" "$provisioner"; then
    echo "FAIL: Windows VM synchronization must invalidate stale build outputs" >&2
    exit 1
fi
if ! grep -Fq 'CBM_VM_BRANCH' "$ROOT/test-infrastructure/vm/README.md"; then
    echo "FAIL: Windows VM branch override is undocumented" >&2
    exit 1
fi
if ! grep -Fq 'pushed base commit' "$ROOT/test-infrastructure/vm/README.md"; then
    echo "FAIL: Windows VM sync pushed-base prerequisite is undocumented" >&2
    exit 1
fi
container_driver="$ROOT/test-infrastructure/run.sh"
if grep -Fq 'All platforms passed' "$container_driver"; then
    echo "FAIL: container/Wine driver must not claim the real-Windows gate passed" >&2
    exit 1
fi
if ! grep -Fq 'Real-Windows gate remains' "$container_driver"; then
    echo "FAIL: container/Wine driver must print the remaining real-Windows gate" >&2
    exit 1
fi

echo "Windows VM worktree manifest contract passed"
