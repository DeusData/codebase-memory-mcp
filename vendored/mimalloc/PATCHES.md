# Vendored mimalloc: source pin and local patches

## Source pin

- Upstream: <https://github.com/microsoft/mimalloc>
- Tag: `v3.5.3` (annotated tag object `deac3dcf5f0614884013d411a5c07e16bec96c9f`)
- Commit: `d4881d338125e1cb7c47ba4cfb398d6f7c0c8d45`
  (tree `fbc4e8f72287d127c079ada5ede870b7d63e55f5`)
- Vendored: `LICENSE`, every file under `include/` and every file under `src/`.
  Nothing else (no build files, tests or docs).

v3.5.3 is upstream's latest published release at the time of the pin
(GitHub release of 2026-09-17, on the `main3` branch).

v3.5.3 contains upstream commit `b2226c195fe884223f6d6f68ea52df476e57e5c2`
(mimalloc #1341, first released in v3.4.4): `free(NULL)` must work before the
allocator is initialized. With 3.3.2's release build, glibc 2.44's
`newlocale()` freed NULL during libstdc++ start-up, before mimalloc's
constructor, and every Linux binary died with SIGSEGV (reported with a
reproduction by Felix Rath in #2562). The preinit fixture in
`tests/test_prod_allocator_contract.sh` pins it.

## Verifying the vendored bytes

Every vendored file is byte-identical to the tag except the two hunks below.
Re-check from a fresh bare clone (nothing in it is built or run):

```sh
git clone --bare https://github.com/microsoft/mimalloc.git /tmp/mimalloc.git
git --git-dir=/tmp/mimalloc.git rev-parse 'v3.5.3^{commit}'   # d4881d33...
for f in $(git --git-dir=/tmp/mimalloc.git ls-tree -r --name-only v3.5.3 -- LICENSE include src); do
  [ "$(git --git-dir=/tmp/mimalloc.git rev-parse "v3.5.3:$f")" = \
    "$(git hash-object "vendored/mimalloc/$f")" ] || echo "differs: $f"
done
```

The expected output is exactly the two files named below, and
`vendored/mimalloc/include` plus `vendored/mimalloc/src` must hold no file the
tag lacks.

## Local patches (reapply on every refresh)

Each patch is pinned by a check that fails when the patch is dropped.

### 1. macOS decommit always recommits: `src/prim/unix/prim.c`

In `_mi_prim_decommit`, macOS takes the recommit path that upstream reserves for
`MI_DEBUG` / `MI_SECURE>2` builds, so a purged range is not re-accounted as a
whole by `MADV_FREE_REUSE` when the arena reuses it. This cut the indexer's peak
`phys_footprint` substantially (measurements in the inline comment and in commit
63fb7b3d; re-measured on v3.5.3 when this pin was taken). Linux and Windows keep
upstream's behaviour.

Pinned by: the decommit fixture in `tests/test_prod_allocator_contract.sh`
(`needs_recommit` must be true on macOS and upstream's false elsewhere).

### 2. Comment wording: `src/prim/osx/alloc-override-zone.c`

Comment only, no code change: "crash in fork()." reads "crash while forking.".
`scripts/security-vendored.sh` rejects call spellings such as `fork(` anywhere
in vendored C, comments included, so the upstream sentence fails that gate.
(Wording first proposed in #2562.)

Pinned by: `scripts/security-vendored.sh` itself, which reports the pristine
upstream line as a blocked subprocess call.

## Build configuration

`Makefile.cbm` compiles the production object with `-DMI_PROFILE=0`, which
leaves out mimalloc's allocation-sampling profiler (on by default since 3.5.2)
and its sample countdown on the allocation path. Pinned by: the banner fixture
in `tests/test_prod_allocator_contract.sh` (a profiling build prints a
`profiling` line). The test build keeps upstream's default.

## Dropped patches

- Reproducible version banner (`src/options.c`, carried from 1f674c80 up to
  3.4.4): upstream removed `__DATE__`/`__TIME__` from the banner in v3.4.5
  (mimalloc #1355). The banner fixture in
  `tests/test_prod_allocator_contract.sh` still asserts that the first line is
  exactly `vX.Y.Z` with no build clock, so a future refresh cannot bring it
  back unnoticed.

## Refresh procedure

1. Replace `LICENSE`, `include/` and `src/` with the new tag's files, deleting
   files the tag no longer has, and verify the staged tree against the tag
   before patching.
2. Reapply the patches above, or drop the ones upstream has absorbed (and move
   them to "Dropped patches" with the upstream reference).
3. Update the pin in this file and `versionInfo` in
   `scripts/ci/generate-sbom.py`, then run `scripts/security-vendored.sh --update`.
4. After `make -f Makefile.cbm cbm`, run
   `bash tests/test_prod_allocator_contract.sh build/c` on Linux and macOS
   (`scripts/test.sh` runs it on every CI leg), and `scripts/security-vendored.sh`.
