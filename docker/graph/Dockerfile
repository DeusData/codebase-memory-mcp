# cbm-graph: build a repository's code graph into Neo4j from a container.
#
#   docker build -f docker/graph/Dockerfile -t cbm-graph:local .
#
# See docker/graph/README.md for running it (cbm-graph.ps1 on Windows).

# ── Build ────────────────────────────────────────────────────────────
FROM gcc:14 AS build
RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN make -f Makefile.cbm cbm BUILD_DIR=/build -j"$(nproc)" \
 && /build/codebase-memory-mcp --version

# ── Runtime ──────────────────────────────────────────────────────────
# gcc:14 is Debian 13; the binary needs its newer libstdc++/libgcc_s.
FROM debian:trixie-slim
RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends git rsync zlib1g ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/lib64/libstdc++.so.6* /usr/local/lib64/libgcc_s.so.1 /usr/local/lib64/
RUN echo /usr/local/lib64 > /etc/ld.so.conf.d/gcc14.conf && ldconfig
COPY --from=build /build/codebase-memory-mcp /usr/local/bin/codebase-memory-mcp
COPY docker/graph/cbm-graph.sh /usr/local/bin/cbm-graph
RUN sed -i 's/\r$//' /usr/local/bin/cbm-graph && chmod 0755 /usr/local/bin/cbm-graph

# Index cache (SQLite sidecars + working copies) lives in /data: mount a
# named volume there so re-runs are incremental.
ENV HOME=/data/home
VOLUME ["/data"]
ENTRYPOINT ["cbm-graph"]
