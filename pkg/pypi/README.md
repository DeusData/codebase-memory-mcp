# codebase-memory-mcp

mcp-name: io.github.DeusData/codebase-memory-mcp

**Fast code intelligence engine for AI coding agents.** Indexes an average repository in milliseconds, the Linux kernel (28M LOC) in 3 minutes. Answers structural queries in under 1ms.

This Python wrapper downloads the selected `codebase-memory-mcp` runtime set from [GitHub Releases](https://github.com/DeusData/codebase-memory-mcp/releases) on first run and verifies it before publishing it in your OS cache directory. The standard set contains the native executable and authenticated integration asset; `CBM_VARIANT=ui` additionally selects the content-addressed UI pack.

## Installation

```bash
pip install codebase-memory-mcp
# or
pipx install codebase-memory-mcp
```

To use the UI variant, set `CBM_VARIANT=ui` when invoking the wrapper (and consistently for any package-managed update or reinstall).

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
