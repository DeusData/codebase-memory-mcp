#ifndef CBM_PERSONAL_MEMORY_H
#define CBM_PERSONAL_MEMORY_H

#include "git/git_context.h"
#include "store/store.h"
#include <stdbool.h>
#include <yyjson/yyjson.h>

struct cbm_config;

bool cbm_memory_enabled(struct cbm_config *cfg);
const char *cbm_memory_resolve_dir(struct cbm_config *cfg);
char *cbm_memory_db_path(struct cbm_config *cfg, bool create_dir);
cbm_store_t *cbm_memory_open(struct cbm_config *cfg, char **out_path);
cbm_store_t *cbm_memory_open_query(struct cbm_config *cfg, char **out_path);

const char *cbm_memory_current_branch(const cbm_git_context_t *ctx);
const char *cbm_memory_default_branch(const cbm_git_context_t *ctx);
char *cbm_memory_repo_id(const char *project, const char *root_path, const cbm_git_context_t *ctx);
char *cbm_memory_doc_key(const char *repo_id, const char *branch, const char *doc_type);

void cbm_memory_add_settings_json(struct cbm_config *cfg, yyjson_mut_doc *doc,
                                  yyjson_mut_val *root, const char *db_path);
void cbm_memory_add_list_json(cbm_store_t *store, const char *repo_id, yyjson_mut_doc *doc,
                              yyjson_mut_val *root);

#endif /* CBM_PERSONAL_MEMORY_H */
