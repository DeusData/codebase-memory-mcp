#!/usr/bin/env bash
# MemorySanitizer lane — the canonical entry, used by every venue.
#
# MSan needs every linked library instrumented or reads of memory those
# libraries wrote report as uninitialized, so the lane runs inside an image
# carrying an MSan-built libc++/libc++abi/libunwind and zlib
# (test-infrastructure/Dockerfile.msan). Building that image is the expensive
# part, hence the buildx layer cache.
#
# Locally the same lane is reached through the compose service:
#   ./test-infrastructure/run.sh msan
# which shares scripts/msan.sh with this path — one harness, both venues.
#
# Usage: scripts/ci/msan-lane.sh [build|run|all]   (default: all)
set -euo pipefail

cd "$(dirname "$0")/../.."

CACHE_DIR="${MSAN_BUILDX_CACHE:-/tmp/.buildx-msan}"
IMAGE="${MSAN_IMAGE:-cbm-msan:ci}"
BUILDER="${MSAN_BUILDER:-msan-builder}"
MODE="${1:-all}"

build_image() {
    echo "=== MSan image (buildx, cached layers) ==="
    # A builder may already exist from a previous step or a retried job.
    docker buildx create --use --name "$BUILDER" 2>/dev/null || docker buildx use "$BUILDER"
    docker buildx build \
        --cache-from "type=local,src=$CACHE_DIR" \
        --cache-to "type=local,dest=$CACHE_DIR,mode=max" \
        -f test-infrastructure/Dockerfile.msan \
        -t "$IMAGE" --load test-infrastructure/
}

run_suite() {
    # MSAN_EXCLUDE is intentionally NOT passed, so the container inherits the
    # single authoritative list defined in scripts/msan.sh (which documents each
    # excluded suite and the cause it is excluded for).
    #
    # This leg used to force it empty to settle whether the local arm64
    # exclusions were an aarch64 artifact. It answered that: the five
    # deep-recursion suites overflow on x86-64 too, `cli` fails for an unrelated
    # install-path reason, and `incremental` was a shadow-memory RSS artifact
    # that is now fixed in the test rather than skipped. Keeping the override
    # would re-red the gate for causes already recorded, so the question is
    # closed and the venues share one list. scripts/msan.sh still warns loudly
    # that the lane is partial, which is the honest signal to keep.
    echo "=== MSan suite (exclusions per scripts/msan.sh) ==="
    docker run --rm -v "$PWD:/src" -w /src "$IMAGE"
}

case "$MODE" in
    build) build_image ;;
    run)   run_suite ;;
    all)   build_image; run_suite ;;
    *)     echo "usage: $0 [build|run|all]" >&2; exit 2 ;;
esac
