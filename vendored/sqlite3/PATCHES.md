# Local patches to the vendored SQLite amalgamation

Reapply every patch below after refreshing the amalgamation, then update
`scripts/vendored-checksums.txt` (`shasum -a 256 vendored/sqlite3/sqlite3.c`).

## 1. Unix VFS path ceiling: `MAX_PATHNAME` 512 → 4096

```c
#define MAX_PATHNAME 4096
```

Upstream's Unix VFS caps full database paths at 512 bytes, while CBM
supports 4 KiB paths everywhere else (cache roots under deep home
directories, long project paths on Linux). With the upstream value,
opening a store whose absolute path exceeds 512 bytes fails with
`SQLITE_CANTOPEN`. 4096 matches `PATH_MAX` on Linux and CBM's own path
buffers. Windows and the other VFS layers are unaffected (they do not
use `MAX_PATHNAME`).
