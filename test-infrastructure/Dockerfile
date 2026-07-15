# Mirrors the Ubuntu CI environment exactly:
#   - Ubuntu 24.04 (same as GitHub Actions ubuntu-latest / ubuntu-24.04-arm)
#   - GCC (system default) with ASan + UBSan + LeakSanitizer
#   - libsqlite3-dev + zlib1g-dev (same as CI "Install deps" step)
#
# Build:  docker build -t cbm-test test-infrastructure/
# Run:    docker run --rm -v $(pwd):/src cbm-test

FROM ubuntu:noble

# Minimal: gcc + zlib only. sqlite3 is vendored (compiled from source with ASan).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make \
    zlib1g-dev \
    pkg-config \
    python3 \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Default: run test.sh with GCC (mirrors CI exactly)
ENTRYPOINT ["scripts/test.sh"]
CMD ["CC=gcc", "CXX=g++"]
