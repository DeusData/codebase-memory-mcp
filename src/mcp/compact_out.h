/* compact_out.h — compact tree emission helpers.
 *
 * Tool responses are consumed by LLM agents, where every byte is context
 * tokens. The tree format declares tabular fields once in a header and
 * streams rows line by line, removing repeated-key overhead while retaining
 * scalar key-value lines and explicit table lengths.
 *
 * Quoting: a cell/scalar is double-quoted iff it is empty, has leading or
 * trailing whitespace, contains a comma, quote, newline, or CR, or would
 * read as a non-string literal (true/false/null/number). Quotes and
 * backslashes are escaped JSON-style; newlines become \n.
 */
#ifndef CBM_MCP_COMPACT_OUT_H
#define CBM_MCP_COMPACT_OUT_H

#include <stdbool.h>
#include <stddef.h>

/* Minimal growing string buffer (OOM-safe: sticky flag, finish returns NULL). */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
    bool oom;
} cbm_sb_t;

void cbm_sb_init(cbm_sb_t *sb);
void cbm_sb_append_n(cbm_sb_t *sb, const char *s, size_t n);
void cbm_sb_append(cbm_sb_t *sb, const char *s);
/* Returns the heap buffer (caller frees) and resets sb. NULL on OOM. */
char *cbm_sb_finish(cbm_sb_t *sb);
void cbm_sb_free(cbm_sb_t *sb);

/* Make an arbitrary byte string safe for both standard JSON and the compact
 * tree without losing identity. Valid UTF-8 is left untouched (`*encoded` is
 * NULL). Invalid input becomes `@bytes:<lowercase hex>`; valid strings that
 * begin with the reserved `@bytes:` or `@utf8:` prefixes become
 * `@utf8:<original>`. The caller frees a non-NULL `*encoded`. */
bool cbm_output_encode_text(const char *data, size_t len, char **encoded, size_t *encoded_len);

/* `key: value` scalar lines (top-level, no indent). */
void cbm_tree_scalar_str(cbm_sb_t *sb, const char *key, const char *val);
void cbm_tree_scalar_int(cbm_sb_t *sb, const char *key, long long v);
void cbm_tree_scalar_bool(cbm_sb_t *sb, const char *key, bool v);

/* `key[n]{col1,col2,...}:` table header; rows follow at 2-space indent. */
void cbm_tree_table_header(cbm_sb_t *sb, const char *key, int n, const char *const *cols,
                           int ncols);

/* Row cells: call row_begin, then cell_* per column (first=true for the
 * first cell), then row_end. Empty/NULL strings emit as empty cells. */
void cbm_tree_row_begin(cbm_sb_t *sb);
void cbm_tree_cell_str(cbm_sb_t *sb, const char *val, bool first);
void cbm_tree_cell_int(cbm_sb_t *sb, long long v, bool first);
void cbm_tree_cell_real(cbm_sb_t *sb, double v, bool first);
void cbm_tree_cell_bool(cbm_sb_t *sb, bool v, bool first);
void cbm_tree_row_end(cbm_sb_t *sb);

/* Emit a flat string table without abbreviating cell values. `cells` is
 * row-major with nrows*ncols entries. */
void cbm_tree_table_rows(cbm_sb_t *sb, const char *key, int nrows, const char *const *cols,
                         int ncols, const char *const *cells);

/* Mixed string/literal variant. Literal columns (numbers/booleans) retain
 * their unquoted types. NULL string_cols means every column is a string. */
void cbm_tree_table_rows_typed(cbm_sb_t *sb, const char *key, int nrows, const char *const *cols,
                               int ncols, const char *const *cells, const bool *string_cols);

/* Semantic prefix-directory variant. Only columns explicitly marked in
 * prefix_cols may be factored, so labels, diagnostics, and arbitrary query
 * strings are never rewritten. The directory activates only when the exact
 * rendering saves at least 15% and 64 bytes and a conservative model-neutral
 * token-shape proxy saves at least 1%. */
void cbm_tree_table_rows_profiled(cbm_sb_t *sb, const char *key, int nrows, const char *const *cols,
                                  int ncols, const char *const *cells, const bool *string_cols,
                                  const bool *prefix_cols);

/* Convert an arbitrary JSON value to the compact tree representation used by
 * lean MCP responses. Homogeneous object arrays become header-once tables;
 * nested values remain lossless JSON cells. Returns a heap string. */
char *cbm_json_to_tree(const char *json);

#endif /* CBM_MCP_COMPACT_OUT_H */
