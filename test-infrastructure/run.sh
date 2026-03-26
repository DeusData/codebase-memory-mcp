#!/bin/bash
# Local CI — test all platforms before pushing.
#
# Coverage:
#   Linux arm64:  test (ASan+LeakSan) + build (-O2)  [native, fast]
#   Linux amd64:  test + build                        [QEMU, slower]
#   Windows:      cross-compile with mingw-w64        [compile-check]
#   macOS:        run natively (not in Docker)
#
# Usage:
#   ./test-infrastructure/run.sh              # Linux test+build + Windows compile
#   ./test-infrastructure/run.sh all          # above + amd64
#   ./test-infrastructure/run.sh windows      # Windows cross-compile only
#   ./test-infrastructure/run.sh test         # Linux arm64 test only
#   ./test-infrastructure/run.sh build        # Linux arm64 build only
#   ./test-infrastructure/run.sh lint         # clang-format + cppcheck
#   ./test-infrastructure/run.sh shell        # debug shell

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/test-infrastructure/docker-compose.yml"

case "${1:-full}" in
    full)
        echo "=== Linux arm64: test + build ==="
        $COMPOSE run --rm test
        $COMPOSE run --rm build
        echo "=== Linux arm64: smoke test ==="
        $COMPOSE run --rm smoke
        echo "=== Windows: cross-compile ==="
        $COMPOSE run --rm build-windows
        echo "=== All passed ==="
        ;;
    test)
        echo "=== Linux arm64: test (ASan + LeakSanitizer) ==="
        $COMPOSE run --rm test
        ;;
    build)
        echo "=== Linux arm64: production build (-O2 -Werror) ==="
        $COMPOSE run --rm build
        ;;
    smoke)
        echo "=== Linux arm64: smoke test (build + run all 7 phases) ==="
        $COMPOSE run --rm smoke
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
        $COMPOSE run --rm test-amd64
        $COMPOSE run --rm build-amd64
        ;;
    all)
        echo "=== Linux arm64: test + build + smoke ==="
        $COMPOSE run --rm test
        $COMPOSE run --rm build
        $COMPOSE run --rm smoke
        echo "=== Linux amd64: test + build + smoke ==="
        $COMPOSE run --rm test-amd64
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
    *)
        echo "Usage: $0 {full|test|build|smoke|windows|amd64|all|lint|shell}"
        exit 1
        ;;
esac
