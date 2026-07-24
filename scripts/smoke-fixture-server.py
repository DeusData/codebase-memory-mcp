#!/usr/bin/env python3
"""Serve smoke-test release artifacts from a race-free ephemeral port."""

from __future__ import annotations

import argparse
import functools
import http.server
import os
import pathlib
import tempfile


def publish_port(port_file: pathlib.Path, port: int) -> None:
    """Atomically publish the assigned port after the listening socket exists."""
    port_file.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="ascii",
        dir=port_file.parent,
        prefix=f".{port_file.name}.",
        delete=False,
    ) as temporary:
        temporary.write(f"{port}\n")
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = pathlib.Path(temporary.name)
    try:
        os.replace(temporary_path, port_file)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True, type=pathlib.Path)
    parser.add_argument("--port-file", required=True, type=pathlib.Path)
    parser.add_argument("--bind", default="127.0.0.1")
    args = parser.parse_args()

    directory = args.directory.resolve(strict=True)
    if not directory.is_dir():
        parser.error(f"--directory is not a directory: {directory}")

    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler,
        directory=str(directory),
    )
    with http.server.ThreadingHTTPServer((args.bind, 0), handler) as server:
        publish_port(args.port_file, server.server_port)
        print(
            f"smoke fixture server: http://{args.bind}:{server.server_port} "
            f"from {directory}",
            flush=True,
        )
        server.serve_forever()


if __name__ == "__main__":
    main()
