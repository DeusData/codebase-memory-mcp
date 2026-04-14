#ifndef CBM_HELPERS_H
#define CBM_HELPERS_H

#include "cbm.h"

// Extract text of a node from source. Returns arena-allocated string.
char *cbm_node_text(CBMArena *a, TSNode node, const char *source);

// Check if a string is a language keyword (should be skipped as callee/usage).
bool cbm_is_keyword(const char *name, CBMLanguage lang);

// Classify a string literal as URL, config, or neither.
// Returns CBM_STRREF_URL (0), CBM_STRREF_CONFIG (1), or -1 for neither.
int cbm_classify_string(const char *str, int len);

// Check if a name is exported per language convention.
bool cbm_is_exported(const char *name, CBMLanguage lang);

// GDScript-specific export detection for variable nodes.
bool cbm_gdscript_variable_is_exported(TSNode node, const char *source);

// Check if a file is a test file based on path and language.
bool cbm_is_test_file(const char *rel_path, CBMLanguage lang);

// Find the innermost enclosing function node by walking parent chain.
// Returns a null node if none found.
TSNode cbm_find_enclosing_func(TSNode node, CBMLanguage lang);

// Get the QN of an enclosing function, or module_qn if none.
const char *cbm_enclosing_func_qn(CBMArena *a, TSNode node, CBMLanguage lang, const char *source,
                                  const char *project, const char *rel_path, const char *module_qn);

// Cached version: uses ctx->ef_cache to avoid repeated parent-chain walks.
const char *cbm_enclosing_func_qn_cached(CBMExtractCtx *ctx, TSNode node);

// Find a child node by kind string.
TSNode cbm_find_child_by_kind(TSNode parent, const char *kind);

// Check if node kind matches a set of types (NULL-terminated array of strings).
bool cbm_kind_in_set(TSNode node, const char **types);

// Check if node has an ancestor of the given kind, within max_depth levels.
bool cbm_has_ancestor_kind(TSNode node, const char *kind, int max_depth);

// Count nodes of given kinds in subtree (for complexity metric).
int cbm_count_branching(TSNode node, const char **branching_types);

// Is this a module-level node? (not nested inside function/class body)
bool cbm_is_module_level(TSNode node, CBMLanguage lang);

// --- FQN computation ---

// Compute qualified name: project.rel_path_parts.name
char *cbm_fqn_compute(CBMArena *a, const char *project, const char *rel_path, const char *name);

// Module QN (file without name): project.rel_path_parts
char *cbm_fqn_module(CBMArena *a, const char *project, const char *rel_path);

// GDScript synthetic script-anchor helpers.
const char *cbm_gdscript_anchor_name(CBMArena *a, TSNode root, const char *source);
const char *cbm_gdscript_anchor_qn(CBMArena *a, TSNode root, const char *source,
                                   const char *project, const char *rel_path);
const char *cbm_gdscript_normalize_path(CBMArena *a, const char *raw_path, const char *rel_path);
const char **cbm_gdscript_anchor_base_classes(CBMArena *a, TSNode root, const char *source,
                                              const char *rel_path);

// Folder QN: project.dir_parts
char *cbm_fqn_folder(CBMArena *a, const char *project, const char *rel_dir);

#endif // CBM_HELPERS_H
