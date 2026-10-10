# mimalloc vendoring

Sources: [microsoft/mimalloc v3.4.4](https://github.com/microsoft/mimalloc/tree/v3.4.4),
commit `1f06f694972279bbc7ec72902e8570f5784d0fc9`.
Only `include/`, `src/`, and `LICENSE` are vendored.

This release includes the fix for
[mimalloc #1341](https://github.com/microsoft/mimalloc/issues/1341):
`free(NULL)` must work before allocator initialization. Without it, glibc 2.44's
`newlocale()` can crash during libstdc++ startup with CBM's Linux global override.

## Local modifications

`src/options.c`: remove `__DATE__` and `__TIME__` from the version banner so
identical sources can produce reproducible binaries. The patch is marked
`CBM LOCAL PATCH` inline. Reapply it on every upstream refresh.

`src/prim/osx/alloc-override-zone.c`: reword the comment about a child crashing
while forking, avoiding a literal function-call spelling that CBM's vendored
security scanner would mistake for an actual subprocess call. No code changes.

After refreshing, update the mimalloc version in `scripts/ci/generate-sbom.py`
and regenerate `scripts/vendored-checksums.txt` with
`scripts/security-vendored.sh --update`.
