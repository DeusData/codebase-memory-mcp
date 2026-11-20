# Glama MCP directory (glama.ai) check image — NOT required to run the tool.
#
# codebase-memory-mcp needs NO Docker to run. This image exists only so Glama
# can build a sandbox, launch the stdio MCP server, and run its introspection
# checks, which power the directory's score badge. The same image is exercised
# locally and in CI by pkg/glama/verify.sh to guard against drift.
#
# We pull the "-portable" release asset, which is fully statically linked
# (gcc -static) — unlike the standard asset, which dynamically links glibc/
# libstdc++ and would fail on an older base. Because it's static, the runtime
# base image is arbitrary. TARGETARCH is amd64 / arm64, matching the asset names.

FROM debian:bookworm-slim AS fetch
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSL "https://github.com/DeusData/codebase-memory-mcp/releases/latest/download/codebase-memory-mcp-linux-${TARGETARCH}-portable.tar.gz" \
      | tar -xz -C /tmp codebase-memory-mcp LICENSE THIRD_PARTY_NOTICES.md \
 && chmod +x /tmp/codebase-memory-mcp \
 && rm -rf /var/lib/apt/lists/*

FROM debian:bookworm-slim
COPY --from=fetch /tmp/codebase-memory-mcp /usr/local/bin/codebase-memory-mcp
COPY --from=fetch /tmp/LICENSE /tmp/THIRD_PARTY_NOTICES.md /usr/share/doc/codebase-memory-mcp/
ENV CBM_CACHE_DIR=/tmp/cbm
ENTRYPOINT ["codebase-memory-mcp"]
