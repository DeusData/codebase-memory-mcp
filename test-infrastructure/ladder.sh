#!/usr/bin/env bash
# ladder.sh — the full local push gate, with all platform legs OVERLAPPED.
#
# The three platforms are separate scheduling domains (macOS native, the
# Colima container VM, the Windows UTM VM), so running their legs
# sequentially leaves most of the machine idle most of the time. This
# launches lint + the Linux container suite + the Windows VM suite in the
# background, runs the macOS full suite in the foreground, then joins and
# prints one verdict per leg. Each leg streams to its own log so a red leg
# can be inspected without rerunning anything.
#
#   test-infrastructure/ladder.sh
#
# Prerequisites: Colima running (Linux/lint legs), the Windows VM
# provisioned and reachable (vm/provision-windows.sh). A missing
# prerequisite fails that leg loudly — never silently skips it.
set -uo pipefail

cd "$(dirname "$0")/.."
LOGS="${CBM_LADDER_LOGS:-${TMPDIR:-/tmp}/cbm-ladder-$$}"
mkdir -p "$LOGS"

echo "=== local ladder: lint + linux + windows in background, mac in foreground ==="
echo "=== logs: $LOGS ==="

./test-infrastructure/run.sh lint > "$LOGS/lint.log" 2>&1 &
LINT_PID=$!
./test-infrastructure/run.sh test > "$LOGS/linux.log" 2>&1 &
LINUX_PID=$!
(./test-infrastructure/vm/win.sh sync &&
    ./test-infrastructure/vm/win.sh test-par) > "$LOGS/windows.log" 2>&1 &
WIN_PID=$!

scripts/test.sh 2>&1 | tee "$LOGS/mac.log"
MAC_RC=${PIPESTATUS[0]}

wait "$LINT_PID"
LINT_RC=$?
wait "$LINUX_PID"
LINUX_RC=$?
wait "$WIN_PID"
WIN_RC=$?

echo ""
echo "=== ladder verdicts ==="
overall=0
report() {
    local leg="$1" rc="$2" log="$3"
    if [ "$rc" -eq 0 ]; then
        echo "  PASS $leg"
    else
        echo "  FAIL $leg (rc=$rc) — $log"
        echo "  ── last 15 lines ──"
        tail -15 "$log" | sed 's/^/    /'
        overall=1
    fi
}
report "macOS   " "$MAC_RC" "$LOGS/mac.log"
report "lint    " "$LINT_RC" "$LOGS/lint.log"
report "linux   " "$LINUX_RC" "$LOGS/linux.log"
report "windows " "$WIN_RC" "$LOGS/windows.log"
exit "$overall"
