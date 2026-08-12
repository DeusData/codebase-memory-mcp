#!/usr/bin/env bash
#
# memlab.sh — deterministic memory-attribution run for #581.
#
# The soak answers "did memory grow over eight minutes"; this answers "which
# allocation sites retained it", in about a minute, with a fixed request count
# so two runs are directly comparable. Everything that made soak numbers hard
# to compare — wall-clock duration, background reindexes, crash-recovery
# phases, throughput differences between builds — is deliberately absent.
#
# Usage: scripts/memlab.sh <binary> [requests] [label]
#
# Output: memlab-<label>.jsonl  (profiler records, one block per sample)
#         memlab-<label>.log    (daemon log with mem.census lines)
# Analyse with: scripts/memlab-report.py memlab-<label>.jsonl --census memlab-<label>.log
set -u

BINARY="${1:?usage: memlab.sh <binary> [requests] [label]}"
REQUESTS="${2:-200}"
LABEL="${3:-$(uname -s | tr '[:upper:]' '[:lower:]')}"

if [ ! -x "$BINARY" ]; then
    echo "FAIL: $BINARY is not executable" >&2
    exit 2
fi

WORK=$(mktemp -d 2>/dev/null || mktemp -d -t memlab)

# Native Windows: the server walks the full ancestor chain of both the binary
# and the cache dir and refuses to start when any ancestor grants mutation to
# untrusted SIDs. The repo checkout and the msys /tmp tree both carry such ACEs,
# so the run has to happen from a stamped root under USERPROFILE — the same
# reason soak-test.sh and the guard suites copy their binaries there.
if [[ "$BINARY" == *.exe ]] && command -v cygpath >/dev/null 2>&1 &&
    ! command -v winepath >/dev/null 2>&1; then
    WIN_ROOT=$(mktemp -d "$(cygpath "$USERPROFILE")/cbm-memlab.XXXXXX")
    WIN_ROOT_W="$(cygpath -w "$WIN_ROOT")"
    # Qualify with the domain: a bare name from coreutils `whoami` resolves
    # against the machine first, so on a host whose name equals the user's the
    # grant lands on an empty principal.
    # Identify the account by SID, never by name. icacls resolves a bare name
    # against the machine first, so a host whose name equals the user's grants
    # to an empty principal (#1532); and a USERDOMAIN-qualified name is
    # UNRESOLVABLE on a workgroup machine — "WORKGROUP\test: No mapping between
    # account names and security IDs was done" — which fails the grant outright
    # and silently leaves the tree writable by Authenticated Users. A SID has
    # neither ambiguity, and the SYSTEM/Administrators grants below already use
    # this form. Name lookup remains only as a fallback where PowerShell is
    # unavailable.
    ME="$(powershell.exe -NoProfile -NonInteractive -Command \
        '[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value' 2>/dev/null |
        tr -d '\r\n')"
    case "${ME}" in
    S-1-*) ME="*${ME}" ;;
    *)
        ME="$(whoami | tr -d '\r')"
        if [ -n "${USERDOMAIN:-}" ]; then
            ME="${USERDOMAIN}\\${ME}"
        fi
        ;;
    esac
    MSYS2_ARG_CONV_EXCL='*' icacls "$WIN_ROOT_W" /reset /Q >/dev/null 2>&1 || true
    if ! MSYS2_ARG_CONV_EXCL='*' icacls "$WIN_ROOT_W" /inheritance:r \
        /grant:r "${ME}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' \
        /Q >/dev/null 2>&1; then
        echo "FAIL: could not stamp $WIN_ROOT_W" >&2
        exit 1
    fi
    cp "$BINARY" "$WIN_ROOT/codebase-memory-mcp.exe"
    BINARY="$WIN_ROOT/codebase-memory-mcp.exe"
    WORK="$WIN_ROOT"
    MSYS2_ARG_CONV_EXCL='*' icacls "${WIN_ROOT_W}\\*" /reset /T /C /Q >/dev/null 2>&1 || true
fi
PROFILE_OUT="$PWD/memlab-${LABEL}.jsonl"
RUN_LOG="$PWD/memlab-${LABEL}.log"
rm -f "$PROFILE_OUT" "$RUN_LOG"

cleanup() { rm -rf "$WORK" 2>/dev/null || true; rm -rf "${WIN_ROOT:-}" 2>/dev/null || true; }
trap cleanup EXIT

# A fixed corpus: same file count and content on every platform, so a
# difference in the output cannot come from a difference in the input.
CORPUS="$WORK/corpus"
mkdir -p "$CORPUS/src"
for i in $(seq 1 60); do
    cat > "$CORPUS/src/module_$i.py" <<EOF
class Widget$i:
    """Fixed corpus file $i."""
    def __init__(self, name):
        self.name = name
        self.parts = []

    def attach(self, part):
        self.parts.append(part)
        return self

    def render(self):
        return "-".join(str(p) for p in self.parts)

def build_$i(count):
    widget = Widget$i("w$i")
    for index in range(count):
        widget.attach(index)
    return widget.render()
EOF
done

echo "=== memlab: binary=$BINARY requests=$REQUESTS label=$LABEL ==="
echo "corpus: $(find "$CORPUS" -name '*.py' | wc -l | tr -d ' ') files"

# The Windows binary needs a native path here; an msys /c/... path is not one.
if command -v cygpath >/dev/null 2>&1 && ! command -v winepath >/dev/null 2>&1; then
    mkdir -p "$WORK/cache"
    export CBM_CACHE_DIR="$(cygpath -w "$WORK/cache")"
else
    export CBM_CACHE_DIR="$WORK/cache"
fi
export CBM_MEM_PROFILE=1
export CBM_MEM_PROFILE_OUT="$PROFILE_OUT"
export CBM_MEM_CENSUS=1
export CBM_LOG_LEVEL=info
export CBM_LOG_FORMAT=text
mkdir -p "$CBM_CACHE_DIR"

# Drive the MCP stdio path via the request/response driver (see its header for
# why batching into a closed stdin does not work).
# The VM's python3 is a NATIVE win32 build, so it cannot open msys paths like
# /c/Users/... . Hand it Windows paths, or it starts nothing and the failure
# looks like the server closing stdout.
DRIVE_BINARY="$BINARY"
DRIVE_CORPUS="$CORPUS"
DRIVE_STDERR="$WORK/server-stderr.log"
if command -v cygpath >/dev/null 2>&1 && ! command -v winepath >/dev/null 2>&1; then
    DRIVE_BINARY="$(cygpath -w "$BINARY")"
    DRIVE_CORPUS="$(cygpath -w "$CORPUS")"
    DRIVE_STDERR="$(cygpath -w "$WORK/server-stderr.log")"
fi
python3 "$(dirname "$0")/memlab-drive.py" "$DRIVE_BINARY" "$DRIVE_CORPUS" "$REQUESTS" --tool "${MEMLAB_TOOL:-search_graph}" --idle-seconds "${MEMLAB_IDLE:-0}" --stderr "$DRIVE_STDERR" > "$WORK/drive.out" 2>&1
RC=$?
# With CBM_CACHE_DIR set the process logs to its own file rather than stderr,
# so fold that in or the census series is invisible.
cat "$WORK"/cache/logs/*.log >> "$RUN_LOG" 2>/dev/null || true
cat "$WORK/server-stderr.log" >> "$RUN_LOG" 2>/dev/null || true
RESPONSES=$(sed -n "s/.*served=\\([0-9]*\\).*/\\1/p" "$WORK/drive.out" | head -1); RESPONSES=${RESPONSES:-0}
CENSUS=$(grep -c "mem.census" "$RUN_LOG" 2>/dev/null | head -1); CENSUS=${CENSUS:-0}
SITES=$(grep -c '"site"' "$PROFILE_OUT" 2>/dev/null | head -1); SITES=${SITES:-0}

echo "exit=$RC responses=$RESPONSES census_samples=$CENSUS profile_records=$SITES"
if [ "$RESPONSES" -eq 0 ]; then
    # A harness that hides why it failed is worse than no harness.
    echo "--- driver output ---" >&2
    cat "$WORK/drive.out" 2>/dev/null | tail -15 >&2
fi
if [ "$CENSUS" -eq 0 ]; then
    echo "WARN: no census samples — check CBM_MEM_CENSUS wiring" >&2
fi
if [ "$SITES" -eq 0 ]; then
    # Not fatal, but never silent: on macOS there is no --wrap, so the
    # profiler legitimately has no observation point.
    echo "WARN: no profiler records — expected on macOS (no --wrap); a gap anywhere else" >&2
fi
echo "profile: $PROFILE_OUT"
echo "log:     $RUN_LOG"
