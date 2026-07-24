# Mirrors the Ubuntu CI environment exactly:
#   - Ubuntu 24.04 (same as GitHub Actions ubuntu-latest / ubuntu-24.04-arm)
#   - GCC (system default) with ASan + UBSan + LeakSanitizer
#   - libsqlite3-dev + zlib1g-dev (same as CI "Install deps" step)
#
# Build:  docker build -t cbm-test test-infrastructure/
# Run:    docker run --rm -v $(pwd):/src cbm-test

# Pinned by digest (supply-chain: no floating tags) — multi-arch manifest-list
# digest of ubuntu:noble as of 2026-07-23; bump deliberately, never to a tag.
FROM ubuntu:noble@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90

# Minimal: gcc + zlib only. sqlite3 is vendored (compiled from source with ASan).
# curl + zsh mirror the GitHub runner images: the self-update tests shell out
# to curl, and the shell-activation tests exercise zsh rc files.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make \
    zlib1g-dev \
    pkg-config \
    python3 \
    git \
    curl \
    zsh \
    ccache \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Verified compiler cache: content-keyed (compiler binary + preprocessed
# input), so a hit is provably the identical compilation — stale results are
# impossible by construction. The compose services mount a persistent volume
# at CCACHE_DIR and route compilers through the Debian masquerade dir.
ENV CCACHE_COMPILERCHECK=content
ENV PATH=/usr/lib/ccache:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR /src

# Default: run test.sh with GCC (mirrors CI exactly)
ENTRYPOINT ["scripts/test.sh"]
CMD ["CC=gcc", "CXX=g++"]
