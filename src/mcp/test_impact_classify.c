/*
 * test_impact_classify.c — the classified ledger of a pinned HEAD inventory
 * (test_impact_classify.h).
 */
#include "mcp/test_impact_classify.h"

#include "discover/discover.h"
#include "foundation/arena.h"
#include "foundation/mem_core.h"
#include "mcp/test_impact_inventory_internal.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

struct cbm_test_impact_ledger {
    CBMArena arena; /* owns this struct and the rows */
    cbm_test_impact_classified_t *rows;
    size_t count;
    unsigned char manifest[32];
    cbm_test_impact_classify_usage_t usage;
};

/* What the inventory's limits leave for this pass after the filter. */
typedef struct {
    uint64_t reads;
    uint64_t content_bytes;
    size_t arena_bytes;
    size_t probe_bytes;
} cls_budget_t;

static bool cls_fail(cni_context *c, cbm_inventory_status_t status, const char *what) {
    if (c->error.status == CBM_INVENTORY_OK) {
        c->error.status = status;
        c->error.file_index = c->file_index;
        snprintf(c->error.diagnostic, sizeof(c->error.diagnostic), "%s", what);
    }
    return false;
}

static const char *cls_basename(const cbm_inventory_path_t *path) {
    const char *name = (const char *)path->data;
    const char *slash = strrchr(name, '/');
    return slash ? slash + 1 : name;
}

static bool cls_budget(cni_context *c, const cbm_test_impact_inventory_t *inventory,
                       cls_budget_t *out) {
    cbm_inventory_limits_t limits;
    cbm_test_impact_inventory_usage_t usage;
    if (!cbm_test_impact_inventory_limits(inventory, &limits) ||
        !cbm_test_impact_inventory_usage(inventory, &usage)) {
        return cls_fail(c, CBM_INVENTORY_STATE, "inventory has no limits");
    }
    if (usage.filter.verified_file_reads_reserved > limits.max_verified_file_reads ||
        usage.filter.verified_content_bytes_reserved > limits.max_verified_content_bytes ||
        usage.arena_budget_used_bytes > limits.max_arena_bytes) {
        return cls_fail(c, CBM_INVENTORY_STATE, "inventory usage exceeds its limits");
    }
    out->reads = limits.max_verified_file_reads - usage.filter.verified_file_reads_reserved;
    out->content_bytes =
        limits.max_verified_content_bytes - usage.filter.verified_content_bytes_reserved;
    out->arena_bytes = limits.max_arena_bytes - usage.arena_budget_used_bytes;
    out->probe_bytes = limits.max_probe_prefix_bytes;
    return true;
}

/* The inventory and the tree describe the same snapshot. */
static bool cls_bound(cni_context *c, const cbm_inventory_filter_view_t *view,
                      cbm_pinned_tree_t *tree) {
    const cbm_pinned_tree_view_t *native = cbm_pinned_tree_view(tree);
    if (!native) {
        return cls_fail(c, CBM_INVENTORY_STATE, "pinned tree is not ready");
    }
    if (native->file_count != view->file_count ||
        memcmp(native->manifest_sha256, view->manifest_sha256, sizeof(view->manifest_sha256)) !=
            0) {
        return cls_fail(c, CBM_INVENTORY_CHANGED, "pinned tree is not the inventory's snapshot");
    }
    return true;
}

static size_t cls_survivors(const cbm_inventory_filter_view_t *view) {
    size_t n = 0;
    for (size_t i = 0; i < view->file_count; i++) {
        n += view->rows[i].disposition == CBM_INVENTORY_FILTER_NEEDS_LANGUAGE;
    }
    return n;
}

/* One survivor: the name, and the verified blob prefix where the name needs
 * it. Charges the read to the budget. */
static bool cls_one(cni_context *c, cni_reader *reader, const cbm_userconfig_t *config,
                    cls_budget_t *budget, cbm_test_impact_ledger_t *ledger, size_t index,
                    CBMLanguage *out) {
    const cbm_inventory_file_t *file = &reader->files[index];
    const char *name = cls_basename(&file->path);
    size_t probe = cbm_language_probe_bytes_with(config, name);
    if (probe == 0) {
        *out = cbm_language_classify_with(config, name, NULL, 0, false, true);
        return true;
    }
    if (probe > budget->probe_bytes || probe > CBM_LANGUAGE_PROBE_MAX) {
        return cls_fail(c, CBM_INVENTORY_LIMIT, "language probe exceeds the probe limit");
    }
    if (budget->reads == 0 || file->content_length > budget->content_bytes) {
        return cls_fail(c, CBM_INVENTORY_LIMIT, "language probe exceeds the read budget");
    }
    unsigned char head[CBM_LANGUAGE_PROBE_MAX];
    size_t copied = 0;
    reader->content_bound = budget->content_bytes;
    cbm_inventory_error_t read_error;
    if (cni_read(reader, index, head, probe, &copied, c->control, &read_error) !=
        CBM_INVENTORY_OK) {
        return false; /* cni_read recorded the error in c */
    }
    budget->reads--;
    budget->content_bytes -= file->content_length;
    ledger->usage.verified_file_reads++;
    ledger->usage.verified_content_bytes += file->content_length;
    *out =
        cbm_language_classify_with(config, name, head, copied, file->content_length > copied, true);
    return true;
}

static bool cls_run(cni_context *c, const cbm_inventory_filter_view_t *view,
                    cbm_pinned_tree_t *tree, const cbm_userconfig_t *config, cls_budget_t *budget,
                    cbm_test_impact_ledger_t *ledger) {
    cni_reader reader = {
        .context = c, .tree = tree, .files = view->files, .file_count = view->file_count};
    for (size_t i = 0; i < view->file_count; i++) {
        const cbm_inventory_filter_row_t *row = &view->rows[i];
        if (row->disposition != CBM_INVENTORY_FILTER_NEEDS_LANGUAGE) {
            continue;
        }
        c->file_index = row->file_index;
        if (row->file_index >= view->file_count || !cni_event(c)) {
            return c->error.status != CBM_INVENTORY_OK ||
                   cls_fail(c, CBM_INVENTORY_STATE, "inventory row names no file");
        }
        CBMLanguage language = CBM_LANG_COUNT;
        if (!cls_one(c, &reader, config, budget, ledger, row->file_index, &language)) {
            return false;
        }
        ledger->rows[ledger->count++] =
            (cbm_test_impact_classified_t){.file_index = row->file_index, .language = language};
    }
    c->file_index = SIZE_MAX;
    return cni_poll(c);
}

cbm_inventory_status_t cbm_test_impact_classify(const cbm_test_impact_inventory_t *inventory,
                                                cbm_pinned_tree_t *tree,
                                                const cbm_userconfig_t *config,
                                                const cbm_inventory_control_t *control,
                                                cbm_test_impact_ledger_t **out,
                                                cbm_inventory_error_t *error) {
    cbm_inventory_error_t initial = {.status = CBM_INVENTORY_OK, .file_index = SIZE_MAX};
    if (out) {
        *out = NULL;
    }
    if (error) {
        *error = initial;
    }
    cni_context c = {.control = control, .error = initial, .file_index = SIZE_MAX};
    const cbm_inventory_filter_view_t *view = cbm_test_impact_inventory_view(inventory);
    cls_budget_t budget = {0};
    cbm_test_impact_ledger_t *ledger = NULL;
    if (!out || !inventory || !tree || !control || !control->deadline_ms) {
        cls_fail(&c, CBM_INVENTORY_INVALID, "invalid classification arguments");
    } else if (!view) {
        cls_fail(&c, CBM_INVENTORY_STATE, "inventory has no view");
    } else if (cni_poll(&c) && cls_bound(&c, view, tree) && cls_budget(&c, inventory, &budget)) {
        size_t survivors = cls_survivors(view);
        size_t need = sizeof(*ledger) + survivors * sizeof(*ledger->rows);
        if (survivors > (SIZE_MAX - sizeof(*ledger)) / sizeof(*ledger->rows) ||
            need > budget.arena_bytes) {
            cls_fail(&c, CBM_INVENTORY_LIMIT, "classified ledger exceeds the arena budget");
        } else {
            CBMArena arena;
            cbm_arena_init_exact(&arena, need);
            ledger = cbm_arena_calloc(&arena, sizeof(*ledger));
            cbm_test_impact_classified_t *rows =
                survivors ? cbm_arena_calloc(&arena, survivors * sizeof(*rows)) : NULL;
            if (!ledger || (survivors && !rows)) {
                cbm_arena_destroy(&arena);
                ledger = NULL;
                cls_fail(&c, CBM_INVENTORY_OOM, "classified ledger allocation failed");
            } else {
                ledger->arena = arena; /* the ledger lives in its own arena */
                ledger->rows = rows;
                ledger->usage.arena_requested_bytes = need;
                memcpy(ledger->manifest, view->manifest_sha256, sizeof(ledger->manifest));
                if (cls_run(&c, view, tree, config, &budget, ledger)) {
                    *out = ledger;
                    return CBM_INVENTORY_OK;
                }
            }
        }
    }
    cbm_test_impact_ledger_free(ledger);
    if (error) {
        *error = c.error;
    }
    return c.error.status;
}

const cbm_test_impact_classified_t *cbm_test_impact_ledger_rows(
    const cbm_test_impact_ledger_t *ledger, size_t *count) {
    if (count) {
        *count = ledger ? ledger->count : 0;
    }
    return ledger ? ledger->rows : NULL;
}

const unsigned char *cbm_test_impact_ledger_manifest(const cbm_test_impact_ledger_t *ledger) {
    return ledger ? ledger->manifest : NULL;
}

bool cbm_test_impact_ledger_usage(const cbm_test_impact_ledger_t *ledger,
                                  cbm_test_impact_classify_usage_t *out) {
    if (!out) {
        return false;
    }
    memset(out, 0, sizeof(*out));
    if (!ledger) {
        return false;
    }
    *out = ledger->usage;
    return true;
}

void cbm_test_impact_ledger_free(cbm_test_impact_ledger_t *ledger) {
    if (ledger) {
        CBMArena arena = ledger->arena; /* ledger itself is inside it */
        cbm_arena_destroy(&arena);
    }
}

/* ── The runnable registry of the same snapshot ─────────────────── */

/* The ledger languages the declarations cover. The model reads C-family
 * conventions only; a declared language it does not support is reported by
 * the model itself and keeps it from being complete. */
static void reg_languages(const cbm_test_declarations_t *declarations, bool *wanted) {
    static const struct {
        const char *name;
        CBMLanguage language;
    } NAMES[] = {{"c", CBM_LANG_C},      {"C", CBM_LANG_C},     {"cpp", CBM_LANG_CPP},
                 {"c++", CBM_LANG_CPP},  {"C++", CBM_LANG_CPP}, {"cuda", CBM_LANG_CUDA},
                 {"CUDA", CBM_LANG_CUDA}};
    bool c_cbm = false;
    if (cbm_test_declarations_preset(declarations, CBM_TEST_PRESET_C_CBM, &c_cbm) && c_cbm) {
        wanted[CBM_LANG_C] = true;
    }
    int count = 0;
    const cbm_test_declaration_t *items = cbm_test_declarations_items(declarations, &count);
    for (int i = 0; i < count; i++) {
        for (size_t j = 0; items[i].language && j < sizeof(NAMES) / sizeof(NAMES[0]); j++) {
            if (strcmp(items[i].language, NAMES[j].name) == 0) {
                wanted[NAMES[j].language] = true;
            }
        }
    }
}

/* The full verified blob of one ledger file into the model. */
static bool reg_add(cni_context *c, cni_reader *reader, cls_budget_t *budget,
                    cbm_test_model_t *model, const cbm_test_impact_classified_t *row) {
    const cbm_inventory_file_t *file = &reader->files[row->file_index];
    if (budget->reads == 0 || file->content_length > budget->content_bytes ||
        file->content_length > (uint64_t)SIZE_MAX) {
        return cls_fail(c, CBM_INVENTORY_LIMIT, "test registry exceeds the read budget");
    }
    size_t len = (size_t)file->content_length;
    char *text = cbm_alloc(CBM_MEM_CLASS_EXTRACT, len ? len : 1);
    if (!text) {
        return cls_fail(c, CBM_INVENTORY_OOM, "test registry source allocation failed");
    }
    size_t copied = 0;
    reader->content_bound = budget->content_bytes;
    cbm_inventory_error_t read_error;
    bool ok = cni_read(reader, row->file_index, (unsigned char *)text, len, &copied, c->control,
                       &read_error) == CBM_INVENTORY_OK;
    if (ok && copied != len) {
        ok = cls_fail(c, CBM_INVENTORY_CHANGED, "test registry source length changed");
    }
    if (ok) {
        budget->reads--;
        budget->content_bytes -= file->content_length;
        if (!cbm_test_model_add_source_language(model, (const char *)file->path.data,
                                                cbm_language_name(row->language), text, len)) {
            ok = cls_fail(c, CBM_INVENTORY_OOM, "test registry source was not accepted");
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, text);
    return ok;
}

cbm_inventory_status_t cbm_test_impact_registry(
    const cbm_test_impact_ledger_t *ledger, const cbm_test_impact_inventory_t *inventory,
    cbm_pinned_tree_t *tree, const cbm_test_declarations_t *declarations,
    const cbm_inventory_control_t *control, cbm_test_model_t **out, cbm_inventory_error_t *error) {
    cbm_inventory_error_t initial = {.status = CBM_INVENTORY_OK, .file_index = SIZE_MAX};
    if (out) {
        *out = NULL;
    }
    if (error) {
        *error = initial;
    }
    cni_context c = {.control = control, .error = initial, .file_index = SIZE_MAX};
    const cbm_inventory_filter_view_t *view = cbm_test_impact_inventory_view(inventory);
    cls_budget_t budget = {0};
    cbm_test_model_t *model = NULL;
    if (!out || !ledger || !inventory || !tree || !declarations || !control ||
        !control->deadline_ms) {
        cls_fail(&c, CBM_INVENTORY_INVALID, "invalid registry arguments");
    } else if (!view) {
        cls_fail(&c, CBM_INVENTORY_STATE, "inventory has no view");
    } else if (memcmp(ledger->manifest, view->manifest_sha256, sizeof(ledger->manifest)) != 0) {
        cls_fail(&c, CBM_INVENTORY_CHANGED, "ledger is not the inventory's snapshot");
    } else if (cni_poll(&c) && cls_bound(&c, view, tree) && cls_budget(&c, inventory, &budget)) {
        /* What the ledger already read is spent. */
        budget.reads -= budget.reads < ledger->usage.verified_file_reads
                            ? budget.reads
                            : ledger->usage.verified_file_reads;
        budget.content_bytes -= budget.content_bytes < ledger->usage.verified_content_bytes
                                    ? budget.content_bytes
                                    : ledger->usage.verified_content_bytes;
        model = cbm_test_model_new_declarations(declarations);
        if (!model) {
            cls_fail(&c, CBM_INVENTORY_OOM, "test registry could not start");
        } else {
            bool wanted[CBM_LANG_COUNT + 1] = {false};
            reg_languages(declarations, wanted);
            cni_reader reader = {
                .context = &c, .tree = tree, .files = view->files, .file_count = view->file_count};
            bool ok = true;
            for (size_t i = 0; ok && i < ledger->count; i++) {
                const cbm_test_impact_classified_t *row = &ledger->rows[i];
                c.file_index = row->file_index;
                if (row->language < 0 || row->language >= CBM_LANG_COUNT ||
                    !wanted[row->language]) {
                    continue;
                }
                ok = cni_event(&c) && reg_add(&c, &reader, &budget, model, row);
            }
            c.file_index = SIZE_MAX;
            if (ok && !cbm_test_model_finish(model)) {
                ok = cls_fail(&c, CBM_INVENTORY_OOM, "test registry could not finish");
            }
            if (ok && cni_poll(&c)) {
                *out = model;
                return CBM_INVENTORY_OK;
            }
        }
    }
    cbm_test_model_free(model);
    if (error) {
        *error = c.error;
    }
    return c.error.status;
}
