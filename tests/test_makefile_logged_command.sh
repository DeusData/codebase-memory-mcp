#!/usr/bin/env bash
set -euo pipefail

# Regression guard for Makefile.cbm's portable live-log wrapper. The memory and
# leak gates must report the tested command's failure even though tee is the
# pipeline's final process, and must also fail if tee cannot write the report.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
export LC_ALL=C
POSIX_SHELL="${CBM_TEST_POSIX_SHELL:-/bin/sh}"

mkdir "$WORKDIR/log-directory"

cat > "$WORKDIR/probe.mk" <<'MAKEFILE'
include $(ROOT)/Makefile.cbm

.PHONY: probe-success probe-command-failure probe-tee-failure

probe-success:
	$(call run_logged_command,$(WORKDIR)/success.log,sh -c 'printf "success-output\n"; exit 0')

probe-command-failure:
	$(call run_logged_command,$(WORKDIR)/failure.log,sh -c 'printf "failure-output\n"; exit 7')

probe-tee-failure:
	$(call run_logged_command,$(WORKDIR)/log-directory,sh -c 'printf "tee-failure-output\n"; exit 0')
MAKEFILE

MAKE=(make -f "$WORKDIR/probe.mk" ROOT="$ROOT" WORKDIR="$WORKDIR" SHELL="$POSIX_SHELL")

"${MAKE[@]}" probe-success > "$WORKDIR/success.stdout" 2>&1
grep -qx 'success-output' "$WORKDIR/success.stdout"
grep -qx 'success-output' "$WORKDIR/success.log"

status=0
"${MAKE[@]}" probe-command-failure > "$WORKDIR/failure.stdout" 2>&1 || status=$?
if [[ $status -eq 0 ]]; then
    echo "FAIL: logged command exit 7 was masked by tee"
    exit 1
fi
grep -q 'Error 7' "$WORKDIR/failure.stdout"
grep -qx 'failure-output' "$WORKDIR/failure.log"

status=0
"${MAKE[@]}" probe-tee-failure > "$WORKDIR/tee-failure.stdout" 2>&1 || status=$?
if [[ $status -eq 0 ]]; then
    echo "FAIL: tee report-write failure was ignored"
    exit 1
fi
grep -q 'tee-failure-output' "$WORKDIR/tee-failure.stdout"

status_files="$(find "$WORKDIR" -name '*.status.*' -print)"
if [[ -n "$status_files" ]]; then
    echo "FAIL: logged command status sidecar was not removed"
    exit 1
fi

echo "PASS: Makefile logged commands preserve command and tee failures under $POSIX_SHELL"
