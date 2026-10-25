#include "snapshot.h"
#include <stdlib.h>
#include <string.h>

void cbm_resolve_hash_number(cbm_sha256_ctx *hash, uint64_t value) {
    unsigned char bytes[8];
    for (unsigned i = 0; i < 8; i++)
        bytes[i] = (unsigned char)(value >> (i * 8));
    cbm_sha256_update(hash, bytes, sizeof(bytes));
}
void cbm_resolve_hash_text(cbm_sha256_ctx *hash, const char *value) {
    size_t length = value ? strlen(value) : 0;
    cbm_resolve_hash_number(hash, length);
    if (length)
        cbm_sha256_update(hash, value, length);
}
void cbm_resolve_hash_finish(cbm_sha256_ctx *hash, char output[65]) {
    static const char hex[] = "0123456789abcdef";
    uint8_t bytes[32];
    cbm_sha256_final(hash, bytes);
    for (size_t i = 0; i < sizeof(bytes); i++) {
        output[i * 2] = hex[bytes[i] >> 4];
        output[i * 2 + 1] = hex[bytes[i] & 15];
    }
    output[64] = '\0';
}
static void hash_list(cbm_sha256_ctx *hash, const char *const *values) {
    size_t count = 0;
    while (values && values[count])
        count++;
    cbm_resolve_hash_number(hash, count);
    for (size_t i = 0; i < count; i++)
        cbm_resolve_hash_text(hash, values[i]);
}
static int compare_digest(const void *a, const void *b) {
    return memcmp(a, b, 65);
}
static void definition_hash(const CBMDefinition *d, char digest[65]) {
    cbm_sha256_ctx h;
    cbm_sha256_init(&h);
    const char *fields[] = {"declaration",   d->qualified_name, d->name,        d->label,
                            d->file_path,    d->signature,      d->return_type, d->receiver,
                            d->parent_class, d->decl_namespace, d->variant_key, d->variants};
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++)
        cbm_resolve_hash_text(&h, fields[i]);
    cbm_resolve_hash_number(&h, d->start_line);
    cbm_resolve_hash_number(&h, d->end_line);
    hash_list(&h, d->base_classes);
    hash_list(&h, d->decorators);
    hash_list(&h, d->param_names);
    hash_list(&h, d->param_types);
    hash_list(&h, d->return_types);
    cbm_resolve_hash_finish(&h, digest);
}
bool cbm_resolve_snapshot(const CBMFileResult *r, const char *path, CBMLanguage language,
                          cbm_resolve_file_t *out, char extracted[65]) {
    if (!r || !path || !out || !extracted || r->defs.count < 0 || r->imports.count < 0 ||
        r->type_assigns.count < 0 || r->field_types.count < 0)
        return false;
    size_t count = (size_t)r->defs.count + (size_t)r->imports.count +
                   (size_t)r->type_assigns.count + (size_t)r->field_types.count;
    if (count > SIZE_MAX / 65)
        return false;
    char (*digests)[65] = count ? malloc(count * sizeof(*digests)) : NULL;
    if (count && !digests)
        return false;
    size_t at = 0;
    for (int i = 0; i < r->defs.count; i++)
        definition_hash(&r->defs.items[i], digests[at++]);
    for (int i = 0; i < r->imports.count; i++) {
        const CBMImport *item = &r->imports.items[i];
        cbm_sha256_ctx h;
        cbm_sha256_init(&h);
        cbm_resolve_hash_text(&h, "import");
        cbm_resolve_hash_text(&h, item->local_name);
        cbm_resolve_hash_text(&h, item->module_path);
        cbm_resolve_hash_text(&h, item->member);
        cbm_resolve_hash_finish(&h, digests[at++]);
    }
    for (int i = 0; i < r->type_assigns.count; i++) {
        const CBMTypeAssign *item = &r->type_assigns.items[i];
        cbm_sha256_ctx h;
        cbm_sha256_init(&h);
        cbm_resolve_hash_text(&h, "type_assignment");
        cbm_resolve_hash_text(&h, item->var_name);
        cbm_resolve_hash_text(&h, item->type_name);
        cbm_resolve_hash_text(&h, item->enclosing_func_qn);
        cbm_resolve_hash_finish(&h, digests[at++]);
    }
    for (int i = 0; i < r->field_types.count; i++) {
        const CBMFieldType *item = &r->field_types.items[i];
        cbm_sha256_ctx h;
        cbm_sha256_init(&h);
        cbm_resolve_hash_text(&h, "field_type");
        cbm_resolve_hash_text(&h, item->class_qn);
        cbm_resolve_hash_text(&h, item->field_name);
        cbm_resolve_hash_text(&h, item->type_text);
        cbm_resolve_hash_finish(&h, digests[at++]);
    }
    if (count > 1)
        qsort(digests, count, sizeof(*digests), compare_digest);
    cbm_sha256_ctx summary;
    cbm_sha256_init(&summary);
    cbm_resolve_hash_text(&summary, "extracted-bindings-v1");
    cbm_resolve_hash_number(&summary, language);
    cbm_resolve_hash_number(&summary, count);
    cbm_resolve_hash_number(&summary, r->has_error);
    cbm_resolve_hash_number(&summary, r->parse_unusable);
    cbm_resolve_hash_number(&summary, r->parse_incomplete);
    cbm_resolve_hash_number(&summary, r->walk_truncated);
    for (size_t i = 0; i < count; i++)
        cbm_resolve_hash_text(&summary, digests[i]);
    hash_list(&summary, r->exports);
    cbm_resolve_hash_finish(&summary, extracted);
    free(digests);
    *out = (cbm_resolve_file_t){.file_path = path,
                                .module = r->module_qn,
                                .raw_content_identity = r->source_content_id[0],
                                .preprocessed_content_identity = r->source_content_id[1],
                                .configuration_identity = r->source_configuration_id,
                                .extracted_summary = extracted,
                                .raw_length = r->source_content_length[0],
                                .preprocessed_length = r->source_content_length[1],
                                .language = (uint32_t)language,
                                .complete = !r->has_error && !r->parse_unusable &&
                                            !r->parse_incomplete && !r->walk_truncated};
    return true;
}

cbm_resolve_proof_source_t cbm_result_proof_source(const CBMFileResult *result,
                                                   CBMSourceOrigin origin, const char *scope) {
    cbm_resolve_proof_source_t source = {0};
    if (!result || origin < CBM_SOURCE_ORIGIN_RAW || origin > CBM_SOURCE_ORIGIN_PREPROCESSED)
        return source;
    return (cbm_resolve_proof_source_t){.project = result->source_project,
                                        .file = result->source_file,
                                        .module = result->module_qn,
                                        .scope = scope,
                                        .content_identity = result->source_content_id[origin],
                                        .configuration_identity = result->source_configuration_id,
                                        .source_origin = (uint32_t)origin};
}
