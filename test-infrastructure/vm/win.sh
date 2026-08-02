#!/usr/bin/env bash
# win.sh — daily driver for the Windows test VM (real-Windows leg of local CI).
#
# All commands run over ssh (local UTM network only — nothing leaves the
# machine). Config: ~/.claude/cbm-vm/config (CBM_VM_HOST, CBM_VM_USER),
# key: ~/.claude/cbm-vm/id_ed25519. Provision first: provision-windows.sh.
#
# Usage:
#   win.sh status                  # reachability + repo + build state
#   win.sh update                  # fetch+reset repo to pushed branch, rebuild
#   win.sh sync                    # mirror this uncommitted worktree + rebuild
#   win.sh build                   # incremental native build (binary+runner)
#   win.sh test <suite...>         # run test-runner suites (native ARM64)
#   win.sh guards                  # clean UI product build + Windows guards
#   win.sh smoke-install           # real managed-install E2E (Phase 8 class)
#   win.sh smoke-artifact          # release-archive flow (package+extract+smoke)
#   win.sh soak [minutes]          # both CI soak legs: quick + #581 query-leak
#   win.sh sh <command...>         # arbitrary command in CLANGARM64 env
#   win.sh push-file <local> <vm>  # scp one file into the VM (WIP iteration)
#   win.sh test-par                # full suite, parallel on all VM cores
#   win.sh ubsan-build|ubsan-test  # UBSan at CI's x86_64 arch (emulated; works)
#   win.sh trap-ubsan-build|-test  # NATIVE ARM64 UBSan (trap mode, no runtime)
#   win.sh pageheap on|off         # OS heap verification for native runs
set -euo pipefail

# help must not require a configured VM: agents discovering the tooling read
# --help before anything exists. Handled before config/ssh setup.
print_help() {
    cat <<'EOF'
Usage: test-infrastructure/vm/win.sh <command> [args]

Daily driver for the real-Windows leg of local CI (UTM ARM64 VM over ssh).
Every venue-grade command routes through the CANONICAL leg scripts — the same
files CI runs (scripts/build.sh, scripts/test.sh, vm-smoke.sh, soak-legs.sh);
this wrapper only provisions (ssh, clock sync, clean-disk preflight, protected
temp root). Config: ~/.claude/cbm-vm/config (CBM_VM_HOST, CBM_VM_USER,
CBM_VM_HOST_KEY_SHA256); key: ~/.claude/cbm-vm/id_ed25519; first-time setup:
test-infrastructure/vm/provision-windows.sh.

Commands (venue-grade — canonical entries, preflight enforced):
  update                 fetch+reset the VM repo to the pushed branch, rebuild
  sync                   mirror this uncommitted worktree to the VM, rebuild
  build                  scripts/build.sh (CLEAN + content-verified ccache)
  test <suite...>        scripts/test.sh --suites  (iteration mode: incremental
                         rebuild, subset run, seconds; list suites with
                         `win.sh sh 'build/c/test-runner --list-suites'`)
  test-par               scripts/test.sh — THE full Windows ladder leg (clean
                         build + contracts + all suites + guards, as CI runs it)
  guards                 clean UI product build + Windows guard suite (CI shape)
  smoke-install          scripts/build.sh + vm-smoke.sh — exactly PR CI's
                         windows smoke job
  smoke-artifact         package-release.sh → extract → vm-smoke.sh artifact
                         mode — the release archive flow with local bytes
  soak [minutes]         both CI soak legs via scripts/soak-legs.sh (default 10)

Sanitizer variants (documented iteration tools; direct-runner mode):
  ubsan-build|ubsan-test <suite...>       x86_64 UBSan under emulation (full
                                          diagnostics; CI's exact x64 arch)
  trap-ubsan-build|trap-ubsan-test <...>  native ARM64 trap-mode UBSan (a UB
                                          hit is a SIGILL trap; diagnose via
                                          the emulated ubsan pair)
  pageheap on|off        OS heap verification for the native runner (IFEO)

Plumbing (no preflight):
  status                 reachability + repo + build state
  sh <command...>        arbitrary command in the CLANGARM64 shell
  push-file <src> <dst>  scp one file into the VM (WIP iteration)
  help                   this text

Environment:
  CBM_VM_BRANCH          branch for update (default: current local branch)
  CBM_VM_MIN_FREE_GB     preflight free-disk floor (default 14, runner spec)
  CBM_VM_SKIP_PREFLIGHT=1  bootstrap-only escape (checkout predates the script)

Exit codes: 0 success · 2 usage error · other = the canonical leg's own code.
EOF
}
case "${1:-}" in
help | -h | --help) print_help; exit 0 ;;
esac

CONFIG="${HOME}/.claude/cbm-vm/config"
KEY="${HOME}/.claude/cbm-vm/id_ed25519"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# The fixed host-local config is intentionally outside this repository.
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"
HOST="${CBM_VM_HOST:?set CBM_VM_HOST in ~/.claude/cbm-vm/config}"
USER_="${CBM_VM_USER:-test}"
HOST_KEY="${CBM_VM_HOST_KEY_SHA256:?set CBM_VM_HOST_KEY_SHA256 in ~/.claude/cbm-vm/config}"
LOCAL_BRANCH="$(git -C "$ROOT" branch --show-current)"
BRANCH="${CBM_VM_BRANCH:-${LOCAL_BRANCH:-main}}"
# Expand inside the remote MSYS2 shell, not on the macOS host.
# shellcheck disable=SC2016
JOBS='$(nproc)'

# shellcheck source=test-infrastructure/vm/ssh-common.sh
source "$SCRIPT_DIR/ssh-common.sh"
cbm_vm_require_safe_branch "$BRANCH"
cbm_vm_prepare_known_hosts "$HOST" "$HOST_KEY"
WIN_MANIFEST=""
WIN_ARCHIVE=""
WIN_PATCH=""
win_cleanup() {
    [ -z "$WIN_MANIFEST" ] || rm -f -- "$WIN_MANIFEST"
    [ -z "$WIN_ARCHIVE" ] || rm -f -- "$WIN_ARCHIVE"
    [ -z "$WIN_PATCH" ] || rm -f -- "$WIN_PATCH"
    cbm_vm_cleanup_known_hosts
}
trap win_cleanup EXIT
SSH_OPTIONS=(-i "$KEY" -o IdentitiesOnly=yes -o HostKeyAlgorithms=ssh-ed25519 \
             -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$CBM_VM_KNOWN_HOSTS" \
             -o ConnectTimeout=10 -o BatchMode=yes)
SSH=(ssh "${SSH_OPTIONS[@]}" "${USER_}@${HOST}")
SCP=(scp "${SSH_OPTIONS[@]}")

vm() { local env="$1"; shift
      "${SSH[@]}" "C:\\msys64\\msys2_shell.cmd -defterm -no-start -${env} -c \"set -e -o pipefail; $*\""; }
vm_cmd() { "${SSH[@]}" "$@"; } # plain cmd.exe (CI-shaped environment)

# A GitHub runner is ephemeral: every job starts on a fresh image with a
# known-free disk. This VM is long-lived, so each run leaves temp roots and
# ~100 MB staged binary copies behind. Sweep them and assert runner-like free
# space BEFORE any build or run, so the local venue never tests a shape CI
# would not produce — and so a disk that has quietly filled fails here, loudly,
# instead of surfacing as a bogus ERROR_DISK_FULL inside the install path.
# CBM_VM_SKIP_PREFLIGHT=1 exists for ONE case: bootstrapping a VM whose
# checkout predates this script, where `update`/`sync` must run before the
# script can exist there. Sweep manually first — it is not a way to run on a
# disk that failed the gate.
vm_preflight() {
    [ "${CBM_VM_SKIP_PREFLIGHT:-0}" = "1" ] && {
        echo "=== win.sh: preflight SKIPPED (CBM_VM_SKIP_PREFLIGHT=1) ==="
        return 0
    }
    vm_cmd "powershell -NoProfile -ExecutionPolicy Bypass -File \
C:\\cbm\\scripts\\ci\\clean-test-residue.ps1 -MinFreeGB ${CBM_VM_MIN_FREE_GB:-14}"
    # Defender parity: every Windows venue (this VM and the GitHub runners)
    # tests with real-time protection ACTIVE — the same canonical script the
    # CI jobs run, so a drifted-off Defender fails loudly, never silently.
    vm_cmd "powershell -NoProfile -ExecutionPolicy Bypass -File \
C:\\cbm\\scripts\\ci\\ensure-defender.ps1"
}

cmd="${1:-status}"; shift || true
case "$cmd" in
status | help | -h | --help) ;;
*) cbm_vm_sync_windows_clock "${SSH[@]}" ;;
esac
# Inspection and plumbing commands need no clean disk; anything that builds or
# runs on the VM does.
case "$cmd" in
status | sh | push-file | pageheap | help | -h | --help) ;;
*) vm_preflight ;;
esac
case "$cmd" in
status)
    "${SSH[@]}" "echo VM_REACHABLE & ver"
    vm clangarm64 "cd /c/cbm 2>/dev/null && git log --oneline -1 && ls -la build/c/codebase-memory-mcp.exe build/c/test-runner.exe 2>/dev/null || echo 'repo/build missing — run provision-windows.sh'"
    ;;
update)
    vm clangarm64 "cd /c/cbm && git fetch origin ${BRANCH} && git reset --hard FETCH_HEAD && git clean -fdx && git log --oneline -1"
    exec "$0" build
    ;;
sync)
    local_head="$(git -C "$ROOT" rev-parse --verify HEAD)"
    WIN_MANIFEST="$(mktemp "${TMPDIR:-/tmp}/cbm-vm-manifest.XXXXXX")"
    WIN_ARCHIVE="$(mktemp "${TMPDIR:-/tmp}/cbm-vm-worktree.XXXXXX.tar")"
    WIN_PATCH="$(mktemp "${TMPDIR:-/tmp}/cbm-vm-worktree.XXXXXX.patch")"
    git -C "$ROOT" diff --binary --full-index HEAD -- >"$WIN_PATCH"
    cbm_vm_write_untracked_manifest "$ROOT" "$WIN_MANIFEST"
    COPYFILE_DISABLE=1 tar --no-xattrs --no-mac-metadata \
        -C "$ROOT" --null -T "$WIN_MANIFEST" -cf "$WIN_ARCHIVE"
    remote_head="$(vm clangarm64 "cd /c/cbm && git rev-parse --verify HEAD")"
    remote_head="${remote_head//$'\r'/}"
    if [ "$remote_head" != "$local_head" ]; then
        echo "FATAL: Windows VM is at $remote_head, expected local HEAD $local_head; run win.sh update first." >&2
        exit 1
    fi
    vm clangarm64 \
        "cd /c/cbm && git reset --hard '$local_head' && git clean -fdx"
    if [ -s "$WIN_PATCH" ]; then
        "${SSH[@]}" \
            'C:\msys64\msys2_shell.cmd -defterm -no-start -clangarm64 -c "cd /c/cbm && git apply --binary --whitespace=nowarn -"' \
            <"$WIN_PATCH"
    fi
    if [ -s "$WIN_MANIFEST" ]; then
        "${SSH[@]}" \
            'C:\msys64\msys2_shell.cmd -defterm -no-start -clangarm64 -c "cd /c/cbm && tar -xf -"' \
            <"$WIN_ARCHIVE"
    fi
    vm clangarm64 "cd /c/cbm && git status --short --branch"
    win_cleanup
    exec "$0" build
    ;;
build)
    # The canonical build entry, exactly as CI runs it: a CLEAN scripts/build.sh
    # (the single product binary). ccache engages through env.sh's masquerade
    # (content-verified, so a warm cache only accelerates, never goes stale) —
    # NOT via CC='ccache clang', which bypassed the verified-cache env layer.
    # The test-runner is no longer built here: the test leg (scripts/test.sh)
    # builds its own runner, same as CI's test jobs.
    vm clangarm64 "cd /c/cbm && scripts/build.sh CC=clang CXX=clang++ > /tmp/win-build.log 2>&1 && echo BUILD_OK || (echo BUILD_FAIL; tail -20 /tmp/win-build.log; exit 1)"
    ;;
test)
    [ $# -ge 1 ] || { echo "usage: win.sh test <suite...>" >&2; exit 2; }
    # Suites run through the CANONICAL scripts/test.sh --suites (incremental
    # rebuild, subset run — the documented iteration mode), under CI's
    # protected per-user temp root via vm-run-tests.sh, which streams FULL
    # output and refuses to report success without the runner's completion
    # summary. A `| tail -40` here once hid 40 real Windows failures.
    vm clangarm64 "cd /c/cbm && bash test-infrastructure/vm/vm-run-tests.sh $*"
    ;;
guards)
    # Match the Windows CI product build: a clean, embedded-UI product binary.
    # Passing that freshly built artifact to the maintained
    # PowerShell driver prevents an earlier non-UI `win.sh build` from silently
    # turning product guards into precondition skips. BUILD_DIR isolates the
    # clean product build from build/c, which build.sh would otherwise wipe —
    # taking the test-runner and every incremental object with it. The UI build
    # needs the official MSVC Node.js (/c/node, from provision-windows.sh):
    # MSVC-built npm native modules cannot resolve Node-API symbols against
    # MSYS2's mingw node.
    vm clangarm64 "export PATH=/c/node:\$PATH && cd /c/cbm && scripts/build.sh --with-ui CC=clang CXX=clang++ SANITIZE= BUILD_DIR=build/guards"
    # The guards themselves run through plain cmd/PowerShell, exactly like the
    # CI job: under the MSYS2 shell TMP is C:\msys64\tmp, whose ancestry
    # grants mutation rights to Authenticated Users — the daemon's
    # cache-private validation (correctly) refuses caches there, which is a
    # different environment shape than CI's profile-rooted TEMP. Python must
    # be PREPENDED: the Microsoft Store python.exe alias stub lives early in
    # the profile PATH and otherwise shadows any appended interpreter.
    vm_cmd "cd /d C:\\cbm && set PATH=C:\\msys64\\clangarm64\\bin;C:\\msys64\\usr\\bin;%PATH%&& powershell -NoProfile -ExecutionPolicy Bypass -File scripts\\test-windows.ps1 -GuardsOnly -Binary build\\guards\\codebase-memory-mcp.exe -Make C:\\msys64\\usr\\bin\\make.exe"
    ;;
smoke-install)
    # EXACTLY the PR CI smoke job (pr.yml pr-smoke windows): a clean canonical
    # build, then the canonical Windows smoke wrapper. The clean build wipes
    # build/c (including any test-runner) just as CI's separate jobs imply;
    # ccache keeps the rebuild to minutes — parity was the explicit user call
    # over incremental speed here.
    vm clangarm64 "cd /c/cbm && scripts/build.sh CC=clang CXX=clang++ && bash test-infrastructure/vm/vm-smoke.sh"
    ;;
smoke-artifact)
    # The release-archive flow with local bytes: canonical build → the ONE
    # package-release.sh → extract → vm-smoke.sh in artifact mode — the same
    # sequence _build.yml + _smoke.yml run remotely, so archive-layout bugs
    # surface here instead of in a release dry run.
    vm clangarm64 "cd /c/cbm && bash scripts/ci/smoke-artifact.sh windows arm64 CC=clang CXX=clang++"
    ;;
soak)
    duration="${1:-10}"
    case "$duration" in
    ''|*[!0-9]*) echo "usage: win.sh soak [positive-minutes]" >&2; exit 2 ;;
    esac
    if [ "$duration" -le 0 ]; then
        echo "usage: win.sh soak [positive-minutes]" >&2
        exit 2
    fi
    vm clangarm64 "cd /c/cbm && CBM_VM_TEST_LOG=/tmp/win-soak.log bash \
        test-infrastructure/vm/vm-run-tests.sh --soak '$duration'"
    ;;
sh)
    vm clangarm64 "$*"
    ;;
push-file)
    [ $# -eq 2 ] || { echo "usage: win.sh push-file <local-path> <vm-path>" >&2; exit 2; }
    # Windows OpenSSH resolves scp targets natively: use C:/... not /c/...
    dest="${2/#\/c\//C:\/}"
    "${SCP[@]}" "$1" "${USER_}@${HOST}:${dest}"
    ;;
ubsan-build)
    # x86_64 (CI's exact arch) with UBSan, runs under Windows-on-ARM emulation.
    # Validated: UBSan needs no interceptors, so it builds, runs, AND reports
    # correctly under emulation. (ASan does NOT: no aarch64 runtime exists and
    # the x86_64 runtime faults in emulated process-init — ASan stays CI-only.)
    vm clang64 "cd /c/cbm && make -j${JOBS} -f Makefile.cbm CC=clang CXX=clang++ SANITIZE='-fsanitize=undefined -fno-omit-frame-pointer' build/c/test-runner > /tmp/win-ubsan-build.log 2>&1 && echo UBSAN_BUILD_OK || (echo UBSAN_BUILD_FAIL; tail -20 /tmp/win-ubsan-build.log; exit 1)"
    ;;
ubsan-test)
    [ $# -ge 1 ] || { echo "usage: win.sh ubsan-test <suite...>" >&2; exit 2; }
    vm clang64 "cd /c/cbm && CBM_VM_TEST_LOG=/tmp/win-ubsan-test.log bash test-infrastructure/vm/vm-run-tests.sh $*"
    ;;
trap-ubsan-build)
    # NATIVE ARM64 UBSan via trap mode. -fsanitize-trap=undefined needs NO
    # runtime library (which is exactly what aarch64-w64-windows-gnu lacks), so
    # unlike ASan this instruments and runs on native ARM64: a UB hit becomes a
    # bare illegal-instruction trap (SIGILL) instead of a diagnostic. Paired
    # with -fstack-protector-strong for stack-smash coverage the heap tools
    # (PageHeap) miss. This is the native-arch UBSan gate; to see WHICH check
    # fired, reproduce under the emulated `win.sh ubsan-build`/`ubsan-test`,
    # which carries the full runtime + message. BUILD_DIR isolated so it never
    # clobbers the plain test-runner.
    vm clangarm64 "cd /c/cbm && make -j${JOBS} -f Makefile.cbm CC='ccache clang' CXX='ccache clang++' SANITIZE='-fsanitize=undefined -fsanitize-trap=undefined -fstack-protector-strong -fno-omit-frame-pointer' BUILD_DIR=build/trap-ubsan build/trap-ubsan/test-runner > /tmp/win-trap-ubsan-build.log 2>&1 && echo TRAP_UBSAN_BUILD_OK || (echo TRAP_UBSAN_BUILD_FAIL; tail -20 /tmp/win-trap-ubsan-build.log; exit 1)"
    ;;
trap-ubsan-test)
    [ $# -ge 1 ] || { echo "usage: win.sh trap-ubsan-test <suite...>" >&2; exit 2; }
    # A UB trap crashes the runner with SIGILL (exit 132); the harness reports
    # the failing suite so the emulated diagnosis loop can name the check.
    vm clangarm64 "cd /c/cbm && CBM_VM_RUNNER=build/trap-ubsan/test-runner CBM_VM_TEST_LOG=/tmp/win-trap-ubsan-test.log bash test-infrastructure/vm/vm-run-tests.sh $*"
    ;;
pageheap)
    # OS-level heap verification (page-granular overflow/UAF detection) for the
    # native ARM64 test-runner — toolchain-agnostic partial ASan substitute.
    # 'on' enables full PageHeap for test-runner.exe via IFEO; 'off' removes it.
    case "${1:-}" in
    on)
        "${SSH[@]}" "reg add \"HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Image File Execution Options\\test-runner.exe\" /v GlobalFlag /t REG_DWORD /d 0x02000000 /f && reg add \"HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Image File Execution Options\\test-runner.exe\" /v PageHeapFlags /t REG_DWORD /d 0x3 /f && echo PAGEHEAP_ON"
        ;;
    off)
        "${SSH[@]}" "reg delete \"HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Image File Execution Options\\test-runner.exe\" /f && echo PAGEHEAP_OFF"
        ;;
    *)  echo "usage: win.sh pageheap on|off" >&2; exit 2 ;;
    esac
    ;;
test-par)
    # THE Windows ladder test leg: the full canonical scripts/test.sh (clean
    # sanitizer build + contract steps 0a-0j + all suites via the parallel
    # harness + prod-binary guards) — exactly what CI's windows test legs run —
    # under the CI-shaped protected temp root with FULL output (this leg once
    # ran under the MSYS-shared /tmp and piped through `tail -25` — the same
    # truncated-blindness class that hid 40 Windows failures from `test`).
    vm clangarm64 "cd /c/cbm && CBM_VM_TEST_LOG=/tmp/win-test-par.log bash test-infrastructure/vm/vm-run-tests.sh --par"
    ;;
help | -h | --help)
    print_help
    ;;
*)
    echo "win.sh: unknown command '$cmd'. Please consult --help." >&2; exit 2
    ;;
esac
