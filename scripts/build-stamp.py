#!/usr/bin/env python3
"""Stamp the build identity into a linked codebase-memory-mcp image (#2683).

The build fingerprint is the SHA-256 of the image with the 32 bytes that
follow the stamp magic read as zeros (src/foundation/build_stamp.c). This
script computes that digest and writes it into those 32 bytes, so the process
can read its own fingerprint instead of hashing its whole image at startup.

It only reads and writes bytes: the image is never executed, which keeps the
release pipeline's rule that no candidate runs before VirusTotal selection.
The image must contain the magic exactly once. Prints the digest.
"""

from __future__ import annotations

import hashlib
import os
import sys

MAGIC = b"\x89CBM-BUILDSTAMP\x01"
PAYLOAD_LEN = 32


def stamp_digest(data: bytearray) -> tuple[str, list[int]]:
    """Zero every payload in stream order and hash; return digest and offsets."""
    payloads = []
    at = data.find(MAGIC)
    while at != -1:
        start = at + len(MAGIC)
        data[start : start + PAYLOAD_LEN] = bytes(len(data[start : start + PAYLOAD_LEN]))
        payloads.append(start)
        at = data.find(MAGIC, at + 1)
    return hashlib.sha256(data).hexdigest(), payloads


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: build-stamp.py <executable>", file=sys.stderr)
        return 2
    path = argv[1]
    with open(path, "rb") as handle:
        data = bytearray(handle.read())
    digest, payloads = stamp_digest(data)
    if len(payloads) != 1 or payloads[0] + PAYLOAD_LEN > len(data):
        print(f"build-stamp: {path}: expected exactly one stamp slot, found {len(payloads)}",
              file=sys.stderr)
        return 1
    payload = bytes.fromhex(digest)
    fd = os.open(path, os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.pwrite(fd, payload, payloads[0])
        os.fsync(fd)
    finally:
        os.close(fd)
    # Re-read: the stored payload must be the digest of the stamped file.
    with open(path, "rb") as handle:
        stamped = bytearray(handle.read())
    stored = bytes(stamped[payloads[0] : payloads[0] + PAYLOAD_LEN])
    again, _ = stamp_digest(stamped)
    if stored != payload or again != digest:
        print(f"build-stamp: {path}: verification failed after writing", file=sys.stderr)
        return 1
    print(digest)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
