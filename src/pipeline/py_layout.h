#ifndef CBM_PIPELINE_PY_LAYOUT_H
#define CBM_PIPELINE_PY_LAYOUT_H

#include <stdbool.h>
#include <stddef.h>

/* Where a Python project keeps its importable code, as its manifests state it.
 * A file under a stated root is the installed module of that path: src/flask/app.py
 * is flask.app, and `import flask` anywhere in the project names it. Manifests are read
 * as text and nothing in them is run. */

/* pyproject.toml, setup.cfg, setup.py, tox.ini or pytest.ini. */
bool cbm_py_layout_is_manifest(const char *basename);

/* The manifests that can sit in one directory, for a lookup per directory. */
const char *cbm_py_layout_manifest_at(size_t index);

/* Each root the manifest states, as written, relative to its directory. */
typedef void (*cbm_py_layout_emit_fn)(void *context, const char *root, size_t length);
void cbm_py_layout_parse(const char *basename, const char *source, size_t source_len,
                         cbm_py_layout_emit_fn emit, void *context);

/* Each member pattern of a uv workspace this pyproject.toml declares, as written: its members
 * are installed together, so each one's modules are importable from the others. */
void cbm_py_layout_workspace_members(const char *source, size_t source_len,
                                     cbm_py_layout_emit_fn emit, void *context);

/* Whether a project directory matches a member pattern, both relative to the workspace:
 * an exact path, or a final `*` standing for one directory name. */
bool cbm_py_layout_member_matches(const char *pattern, const char *directory);

/* A top-level standard library module. The standard library precedes an installed
 * distribution on sys.path, so such a name never reaches a source root. */
bool cbm_py_layout_is_stdlib(const char *name, size_t length);

#endif
