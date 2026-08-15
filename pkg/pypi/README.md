# codebase-memory-mcp

mcp-name: io.github.DeusData/codebase-memory-mcp

**Fast code intelligence engine for AI coding agents.** Indexes an average repository in milliseconds, the Linux kernel (28M LOC) in 3 minutes. Answers structural queries in under 1ms.

This Python wrapper downloads the selected `codebase-memory-mcp` runtime set from [GitHub Releases](https://github.com/DeusData/codebase-memory-mcp/releases) on first run and verifies it before publishing it in your OS cache directory. The set contains the native executable and authenticated integration asset, with the graph UI always embedded.

## Installation

```bash
pip install codebase-memory-mcp
# or
pipx install codebase-memory-mcp
```

There is one composition per platform: the graph UI ships in every build, so no variant selection is needed.

## Usage

```bash
codebase-memory-mcp install   # configure your coding agents
codebase-memory-mcp --help
```

## Supported platforms

| OS      | Architecture |
|---------|-------------|
| macOS   | arm64, amd64 |
| Linux   | arm64, amd64 |
| Windows | arm64, amd64 |

## Full documentation

See [github.com/DeusData/codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp)
