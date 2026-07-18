#!/usr/bin/env bash
# Local CI — test all platforms before pushing.
#
# Coverage:
#   Linux arm64:    test (ASan+LeakSan) + build (-O2)  [native, fast]
#   Linux amd64:    test + build                        [QEMU, slower]
#   Linux portable: Alpine musl static build + smoke    [portable binary]
#   Windows:        cross-compile with mingw-w64        [compile-check; Wine
#                   CANNOT reproduce real Windows ACL/token/owner semantics —
#                   those need a real Windows VM or the CI legs]
#   macOS:          run natively (not in Docker)
#
# Speed first: test containers run unconstrained by default. Before pushing
# anything that touches timing, scheduling, or subprocess code, run ONE pass
# with CBM_LOCAL_CI_CPUS=4 (CI-fidelity mode, mirrors the 4-core GitHub
# runners) — deadline/starvation failures only reproduce under that
# constraint. ccache persists in named volumes; entries are content-verified
# (stale hits impossible), so warm reruns skip unchanged compilation.
# Run this from the WORKTREE you want tested: the containers mount the repo
# this script resides in.
#
# Usage:
#   ./test-infrastructure/run.sh              # arm64 test+build + portable + Windows
#   ./test-infrastructure/run.sh all          # above + amd64
#   ./test-infrastructure/run.sh portable     # Alpine portable build + smoke only
#   ./test-infrastructure/run.sh windows      # Windows cross-compile only
#   ./test-infrastructure/run.sh test         # Linux arm64 test only (no perf)
#   ./test-infrastructure/run.sh perf         # Linux arm64 perf/incremental only
#   ./test-infrastructure/run.sh build        # Linux arm64 build only
#   ./test-infrastructure/run.sh lint         # clang-format + cppcheck
#   ./test-infrastructure/run.sh shell        # debug shell

# Runtime: any Docker-compatible daemon. On macOS we use Colima (free OSS):
#   brew install colima docker docker-compose docker-buildx
#   ln -sf /opt/homebrew/opt/docker-compose/bin/docker-compose ~/.docker/cli-plugins/docker-compose
#   ln -sf /opt/homebrew/opt/docker-buildx/bin/docker-buildx  ~/.docker/cli-plugins/docker-buildx
#   colima start --vm-type vz --vz-rosetta --cpu "$(sysctl -n hw.ncpu)" --memory 32
# Give the VM ALL cores and generous memory — speed beats resource limits;
# leave ~16 GB to macOS itself. --vz-rosetta is required for fast amd64 legs
# (QEMU otherwise). Autostart at login: brew services start colima.
#
# Monitoring a running leg: docker logs -f <container>  (or tail the log you
# redirected to). Check in regularly instead of waiting blind — suite results
# stream as they finish, and failures print their FAIL sites immediately.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/test-infrastructure/docker-compose.yml"

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

case "${1:-full}" in
    full)
        echo "=== Linux arm64: test + build ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test
        $COMPOSE run --rm build
        echo "=== Linux arm64: smoke test ==="
        $COMPOSE run --rm smoke
        echo "=== Linux portable: Alpine static build + smoke ==="
        $COMPOSE run --rm smoke-portable
        echo "=== Windows: cross-compile ==="
        $COMPOSE run --rm build-windows
        echo "=== All passed ==="
        ;;
    test)
        echo "=== Linux arm64: test (ASan + LeakSanitizer, no perf) ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test
        ;;
    perf)
        echo "=== Linux arm64: perf/incremental tests ==="
        $COMPOSE run --rm test
        ;;
    build)
        echo "=== Linux arm64: production build (-O2 -Werror) ==="
        $COMPOSE run --rm build
        ;;
    smoke)
        echo "=== Linux arm64: smoke test (build + run all phases) ==="
        $COMPOSE run --rm smoke
        ;;
    portable)
        echo "=== Linux portable: Alpine static build + smoke ==="
        $COMPOSE run --rm smoke-portable
        ;;
    portable-test)
        echo "=== Linux portable: Alpine test (ASan + LeakSan) ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test-portable
        ;;
    windows)
        echo "=== Windows: cross-compile + smoke (Wine) ==="
        $COMPOSE run --rm smoke-windows
        ;;
    smoke-windows)
        echo "=== Windows: smoke test (cross-compile + Wine) ==="
        $COMPOSE run --rm smoke-windows
        ;;
    soak-windows)
        echo "=== Windows: soak test (cross-compile + Wine, 10 min) ==="
        $COMPOSE run --rm soak-windows
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
        echo "=== Linux portable: Alpine static build + smoke ==="
        $COMPOSE run --rm smoke-portable
        echo "=== Linux amd64: test + build + smoke ==="
        $COMPOSE run --rm -e CBM_SKIP_PERF=1 test-amd64
        $COMPOSE run --rm build-amd64
        $COMPOSE run --rm smoke-amd64
        echo "=== Windows: cross-compile + smoke (Wine) ==="
        $COMPOSE run --rm smoke-windows
        echo "=== All platforms passed ==="
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
        echo "Usage: $0 {full|test|build|smoke|portable|portable-test|windows|amd64|all|lint|shell|shell-alpine}"
        exit 1
        ;;
esac
