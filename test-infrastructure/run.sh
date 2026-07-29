#!/usr/bin/env bash
# Containerized local-CI legs. Before pushing, pair these with native macOS
# tests and the real-Windows VM gate documented below.
#
# Coverage:
#   Linux arm64:    test (ASan+LeakSan) + build (-O2)  [native, fast]
#   Linux amd64:    test + build                        [QEMU, slower]
#   Linux portable: Alpine musl static build + smoke    [portable binary]
#   Windows:        cross-compile + Wine version check  [fast check only; use
#                   vm/win.sh for mandatory real-Windows verification]
#   macOS:          run natively (not in Docker)
#
# Full power, always: the test containers run unconstrained — the suite is
# built to be core-count-independent for correctness (timing/scheduling/
# subprocess suites assert invariants — ordering, bounded-return, RUNNING poll
# state — not wall-clock), so it passes at any parallelism and there is no
# reason to throttle it. The CBM_LOCAL_CI_CPUS knob survives only for the
# smoke/soak services, where deliberate resource starvation is the point; it is
# NOT a pre-push gate for the regular suite. ccache persists in named volumes;
# entries are content-verified
# (stale hits impossible), so warm reruns skip unchanged compilation.
# Run this from the WORKTREE you want tested: the containers mount the repo
# this script resides in.
#
# Usage:
#   ./test-infrastructure/run.sh              # arm64 + portable + Windows cross-compile
#   ./test-infrastructure/run.sh all          # above + amd64 + Windows Wine smoke
#   ./test-infrastructure/run.sh portable     # Alpine portable build + smoke only
#   ./test-infrastructure/run.sh windows      # Windows cross-compile only
#   ./test-infrastructure/run.sh soak-linux   # arm64 soak: quick + query-leak
#   ./test-infrastructure/run.sh soak-windows # native Windows VM, both legs
#   ./test-infrastructure/run.sh test         # Linux arm64 test only (no perf)
#   ./test-infrastructure/run.sh perf         # Linux arm64 perf/incremental only
#   ./test-infrastructure/run.sh tsan         # Linux arm64 ThreadSanitizer race gate
#   ./test-infrastructure/run.sh tsan-amd64   # Linux amd64 ThreadSanitizer race gate
#   ./test-infrastructure/run.sh build        # Linux arm64 build only
#   ./test-infrastructure/run.sh lint         # clang-format + cppcheck
#   ./test-infrastructure/run.sh shell        # debug shell

# Runtime: any Docker-compatible daemon. On macOS we use Colima (free OSS):
#   brew install colima docker docker-compose docker-buildx
#   ln -sf /opt/homebrew/opt/docker-compose/bin/docker-compose ~/.docker/cli-plugins/docker-compose
#   ln -sf /opt/homebrew/opt/docker-buildx/bin/docker-buildx  ~/.docker/cli-plugins/docker-buildx
#   colima start --vm-type vz --vz-rosetta --cpu "$(sysctl -n hw.ncpu)" --memory 32
# vCPUs are NOT a reservation: idle guest cores cost nothing and the macOS
# scheduler shares freely with everything else running. Exposing all cores
# just removes the artificial ceiling a smaller VM would impose on container
# runs — no session seizes anything. Memory is the only semi-reservation,
# hence 32 GB, leaving macOS ample headroom. --vz-rosetta is required for
# fast amd64 legs (QEMU otherwise). Autostart: brew services start colima.
#
# Monitoring a running leg: docker logs -f <container>  (or tail the log you
# redirected to). Check in regularly instead of waiting blind — suite results
# stream as they finish, and failures print their FAIL sites immediately.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/test-infrastructure/docker-compose.yml"

usage() {
    cat <<'EOF'
Usage: test-infrastructure/run.sh [leg]

The Linux (and cross-compile) local-CI ladder, on Colima. Every leg runs the
SAME canonical scripts CI runs (scripts/test.sh, build.sh, smoke-local.sh,
soak-legs.sh) inside pinned containers; this wrapper only picks the container
and platform. A clean-disk preflight (scripts/ci/preflight-docker.sh) runs
first — CBM_SKIP_PREFLIGHT=1 skips it, CBM_PREFLIGHT_ARGS=--deep widens it.

Legs:
  full (default)  arm64: test + build + TSan + smoke + portable smoke
                  + Windows mingw cross-compile check
  all             full + amd64 legs + Windows cross-compile/Wine check
  test|build|smoke|tsan          single arm64 legs
  amd64|test-amd64|tsan-amd64    amd64 legs (tsan-amd64 needs real amd64 HW)
  perf            arm64 incremental-perf leg (CBM_SKIP_PERF unset)
  shell|shell-alpine             interactive debug shell inside the image
  portable|portable-test         Alpine musl static build legs
  smoke-artifact  arm64 artifact-flow smoke: package -> extract -> wrapper
  glibc-floor     ubuntu-22.04 (glibc 2.35): portable smoke + dynamic refusal
  soak-linux      both CI soak legs (quick + #581 query-leak) via
                  scripts/soak-legs.sh; CBM_SOAK_MINUTES per leg (default 10)
  soak-windows    delegates to the real-Windows VM (win.sh soak 10)

Environment:
  CBM_SOAK_MINUTES     soak-linux duration per leg (default 10)
  CBM_LOCAL_CI_CPUS    opt-in CPU cap for smoke/soak starvation checks only
                       (the regular suite always runs full-parallel)
  CBM_SKIP_PREFLIGHT=1 / CBM_PREFLIGHT_ARGS=--deep   preflight controls
  windows|smoke-windows          mingw cross-compile + Wine version check
  lint            containerized cppcheck + clang-format (CI image)
  -h, --help      This text.

Real-Windows legs (test/guards/smoke-install/soak) live in
test-infrastructure/vm/win.sh — Wine is a compile check only.
EOF
}
case "${1:-}" in -h|--help) usage; exit 0 ;; esac

print_real_windows_gate() {
    echo "=== Container/cross-compile legs passed ==="
    echo "=== Real-Windows gate remains: vm/win.sh sync, test, guards, smoke-install ==="
}

if [ "${1:-full}" = "soak-windows" ]; then
    echo "=== Windows: native daemon soak (real Windows VM, 10 min) ==="
    exec "$ROOT/test-infrastructure/vm/win.sh" soak 10
fi

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: no Docker daemon reachable." >&2
    echo "  Start one first — on macOS: colima start --vm-type vz --vz-rosetta --cpu 12 --memory 16" >&2
    echo "  (current context: $(docker context show 2>/dev/null || echo unknown))" >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "ERROR: docker compose plugin missing." >&2
    echo "  brew install docker-compose && ln -sf /opt/homebrew/opt/docker-compose/bin/docker-compose ~/.docker/cli-plugins/docker-compose" >&2
    exit 1
fi

# A GitHub runner starts every job on a fresh image with a known-free disk;
# Colima is long-lived and accumulates exited containers and orphaned volumes.
# Sweep that back to the runner's shape and assert free space before any leg,
# so a disk that has quietly filled fails here rather than surfacing as a bogus
# product failure deep inside an install path. CBM_PREFLIGHT_ARGS=--deep also
# reclaims the build cache; CBM_SKIP_PREFLIGHT=1 opts out for a quick re-run.
if [ "${CBM_SKIP_PREFLIGHT:-0}" != "1" ]; then
    # shellcheck disable=SC2086  # deliberate word-splitting of opt-in flags
    "$ROOT/scripts/ci/preflight-docker.sh" ${CBM_PREFLIGHT_ARGS:-}
fi

case "${1:-full}" in
    full)
        echo "=== Linux arm64: test + build ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test
        $COMPOSE run --rm build
        echo "=== Linux arm64: ThreadSanitizer (data-race gate) ==="
        $COMPOSE run --rm test-tsan
        echo "=== Linux arm64: smoke test ==="
        $COMPOSE run --rm smoke
        echo "=== Linux portable: Alpine static build + smoke ==="
        $COMPOSE run --rm smoke-portable
        echo "=== Linux arm64: artifact-flow smoke ==="
        $COMPOSE run --rm smoke-artifact
        echo "=== Linux glibc-floor (ubuntu-22.04): portable smoke + dynamic refusal ==="
        $COMPOSE run --rm smoke-glibc-floor
        echo "=== Windows: cross-compile ==="
        $COMPOSE run --rm build-windows
        print_real_windows_gate
        ;;
    test)
        echo "=== Linux arm64: test (ASan + LeakSanitizer, no perf) ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test
        ;;
    perf)
        echo "=== Linux arm64: perf/incremental tests ==="
        $COMPOSE run --rm test
        ;;
    tsan)
        echo "=== Linux arm64: ThreadSanitizer (data-race gate) ==="
        $COMPOSE run --rm test-tsan
        ;;
    tsan-amd64)
        # NOTE: TSan's shadow memory is incompatible with x86_64-on-ARM
        # translation (Rosetta/QEMU), so this FATALs ("unexpected memory
        # mapping") on an Apple-Silicon host — it is a real-amd64-hardware /
        # GitHub-CI gate, not a local-on-ARM one. ASan amd64 (test-amd64) runs
        # fine under Rosetta; only TSan's mapping does not.
        echo "=== Linux amd64: ThreadSanitizer (data-race gate; native amd64 only) ==="
        $COMPOSE run --rm test-tsan-amd64
        ;;
    build)
        echo "=== Linux arm64: production build (-O2 -Werror) ==="
        $COMPOSE run --rm build
        ;;
    smoke)
        echo "=== Linux arm64: smoke test (build + run all phases) ==="
        $COMPOSE run --rm smoke
        ;;
    soak-linux)
        # Both CI legs (quick + #581 query-leak). Duration via CBM_SOAK_MINUTES,
        # default 10 to match the dry run. Kept out of `full`/`all` for the same
        # reason soak-windows is: it is a ~20-minute endurance gate, not part of
        # the fast ladder.
        echo "=== Linux arm64: soak (quick + query-leak, ${CBM_SOAK_MINUTES:-10}m each) ==="
        $COMPOSE run --rm soak
        ;;
    portable)
        echo "=== Linux portable: Alpine static build + smoke ==="
        $COMPOSE run --rm smoke-portable
        ;;
    smoke-artifact)
        echo "=== Linux arm64: artifact-flow smoke (package -> extract -> wrapper) ==="
        $COMPOSE run --rm smoke-artifact
        ;;
    glibc-floor)
        echo "=== Linux glibc-floor (ubuntu-22.04): portable smoke + dynamic refusal ==="
        $COMPOSE run --rm build
        $COMPOSE run --rm build-portable
        $COMPOSE run --rm smoke-glibc-floor
        ;;
    portable-test)
        echo "=== Linux portable: Alpine test (ASan + LeakSan) ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test-portable
        ;;
    windows)
        echo "=== Windows: cross-compile + binary version check (Wine) ==="
        $COMPOSE run --rm smoke-windows
        ;;
    smoke-windows)
        echo "=== Windows: binary version check (cross-compile + Wine) ==="
        $COMPOSE run --rm smoke-windows
        ;;
    amd64)
        echo "=== Linux amd64: test + build ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test-amd64
        $COMPOSE run --rm build-amd64
        ;;
    all)
        echo "=== Linux arm64: test + build + smoke ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test
        $COMPOSE run --rm build
        $COMPOSE run --rm smoke
        echo "=== Linux arm64: ThreadSanitizer (data-race gate) ==="
        $COMPOSE run --rm test-tsan
        # amd64 TSan is CI/native-amd64 only (Rosetta can't map TSan shadow);
        # run it with `run.sh tsan-amd64` on real amd64. GitHub CI gates it.
        echo "=== Linux portable: Alpine static build + smoke ==="
        $COMPOSE run --rm smoke-portable
        echo "=== Linux arm64: artifact-flow smoke ==="
        $COMPOSE run --rm smoke-artifact
        echo "=== Linux glibc-floor (ubuntu-22.04): portable smoke + dynamic refusal ==="
        $COMPOSE run --rm smoke-glibc-floor
        echo "=== Linux amd64: test + build + smoke ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test-amd64
        $COMPOSE run --rm build-amd64
        $COMPOSE run --rm smoke-amd64
        echo "=== Windows: cross-compile + binary version check (Wine) ==="
        $COMPOSE run --rm smoke-windows
        print_real_windows_gate
        ;;
    lint)
        echo "=== Linters (clang-format-20 + cppcheck 2.20.0) ==="
        $COMPOSE run --rm lint
        ;;
    shell)
        echo "=== Debug shell (Linux arm64) ==="
        $COMPOSE run --rm --entrypoint bash test
        ;;
    shell-alpine)
        echo "=== Debug shell (Alpine) ==="
        $COMPOSE run --rm --entrypoint bash test-portable
        ;;
    *)
        echo "run.sh: unknown leg '${1:-}'. Please consult --help." >&2
        exit 2
        ;;
esac
