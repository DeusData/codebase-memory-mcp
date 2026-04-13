"""
codebase-memory-mcp — Fast code intelligence engine for AI coding agents.
Downloads and runs the codebase-memory-mcp binary from GitHub Releases.
"""

try:
    from importlib.metadata import version, PackageNotFoundError
    try:
        __version__ = version("codebase-memory-mcp")
    except PackageNotFoundError:
        __version__ = "unknown"
except ImportError:
    __version__ = "unknown"

from ._cli import main

__all__ = ["main", "__version__"]
