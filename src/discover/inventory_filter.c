#include "discover/inventory_internal.h"
#include "discover/discover.h"
#include "discover/discover_policy_internal.h"

#include <string.h>

static bool cif_match(cif_context *c, const cbm_ignore_program_t *program,
                      cbm_inventory_path_t path, bool directory, cbm_ignore_decision_t *decision) {
    *decision = CBM_IGNORE_NO_OPINION;
    if (!program) {
        return true;
    }
    if (!cif_poll(c)) {
        return false;
    }
    cbm_ignore_checked_control_t control = cif_ignore_control(c);
    cbm_ignore_checked_status_t status =
        cbm_ignore_checked_match(c->owner->ignore, program, (const char *)path.data, path.length,
                                 directory, &control, decision, NULL);
    if (!cif_checked(c, status)) {
        return false;
    }
    if (*decision != CBM_IGNORE_REINCLUDED && *decision != CBM_IGNORE_NO_OPINION &&
        *decision != CBM_IGNORE_IGNORED) {
        return cif_fail(c, CBM_INVENTORY_STATE);
    }
    return cif_poll(c);
}

static bool cif_git_opinion(cif_context *c, size_t parent, cbm_inventory_path_t path,
                            bool directory, cbm_ignore_decision_t *decision) {
    *decision = CBM_IGNORE_NO_OPINION;
    while (parent != SIZE_MAX) {
        if (!cif_event(c) || !cif_bytes(c, 128)) {
            return false;
        }
        const cif_directory *d = &c->owner->directories[parent];
        size_t offset = d->path.length ? d->path.length + 1 : 0;
        if (offset > path.length) {
            return cif_fail(c, CBM_INVENTORY_STATE);
        }
        cbm_inventory_path_t local = {path.data + offset, path.length - offset};
        if (!cif_match(c, d->program, local, directory, decision)) {
            return false;
        }
        if (*decision != CBM_IGNORE_NO_OPINION) {
            return true;
        }
        parent = d->parent;
    }
    return true;
}

static bool cif_ignore_reason(cif_context *c, size_t parent, cbm_inventory_path_t path,
                              bool directory, cbm_inventory_filter_reason_t *reason) {
    cbm_ignore_decision_t decision;
    if (!cif_git_opinion(c, parent, path, directory, &decision)) {
        return false;
    }
    if (decision == CBM_IGNORE_IGNORED) {
        *reason = CBM_INVENTORY_FILTER_REASON_GITIGNORE;
        return true;
    }
    if (!cif_match(c, c->owner->cbm_program, path, directory, &decision)) {
        return false;
    }
    if (decision == CBM_IGNORE_IGNORED) {
        *reason = CBM_INVENTORY_FILTER_REASON_CBMIGNORE;
    }
    return true;
}

static const char *cif_basename(const cbm_inventory_filter_t *o, size_t parent,
                                cbm_inventory_path_t path) {
    size_t length = o->directories[parent].path.length;
    return (const char *)path.data + (length ? length + 1 : 0);
}

static bool cif_file_reason(cif_context *c, size_t parent, cbm_inventory_path_t path,
                            cbm_inventory_filter_reason_t *reason) {
    const char *name = cif_basename(c->owner, parent, path);
    if (!cif_poll(c)) {
        return false;
    }
    /* Shared FULL helpers have bounded literal tables; suffix length is cached
     * in discover.c so a 4095-byte name cannot cause repeated full scans. */
    if (cbm_has_ignored_suffix(name, CBM_MODE_FULL)) {
        *reason = CBM_INVENTORY_FILTER_REASON_IGNORED_SUFFIX;
    } else if (cbm_should_skip_filename(name, CBM_MODE_FULL)) {
        *reason = CBM_INVENTORY_FILTER_REASON_SKIP_LIST;
    } else if (cbm_matches_fast_pattern(name, CBM_MODE_FULL)) {
        *reason = CBM_INVENTORY_FILTER_REASON_FAST_PATTERN;
    }
    if (!cif_poll(c)) {
        return false;
    }
    return *reason != CBM_INVENTORY_FILTER_REASON_NONE ||
           cif_ignore_reason(c, parent, path, false, reason);
}

/* The directory is a Python virtual environment: one of the shared markers is
 * a file of the inventory (which holds regular files only, never a link). */
static bool cif_venv(cif_context *c, cbm_inventory_path_t path, bool *venv) {
    *venv = false;
    unsigned char buffer[CIF_PATH_MAX + 1];
    for (size_t m = 0; CBM_DISCOVER_VENV_MARKERS[m] && !*venv; m++) {
        size_t length = strlen(CBM_DISCOVER_VENV_MARKERS[m]);
        /* A longer candidate cannot occur in this fully validated inventory. */
        if (path.length > CIF_PATH_MAX - length - 1) {
            continue;
        }
        if (!cif_copy(c, buffer, path.data, path.length) || !cif_bytes(c, 1)) {
            return false;
        }
        buffer[path.length] = '/';
        if (!cif_copy(c, buffer + path.length + 1, CBM_DISCOVER_VENV_MARKERS[m], length + 1)) {
            return false;
        }
        size_t index = SIZE_MAX;
        if (!cif_find_file(c, (cbm_inventory_path_t){buffer, path.length + 1 + length}, &index)) {
            return false;
        }
        *venv = index != SIZE_MAX;
    }
    return true;
}

/* The same decision as discover.c directory_verdict. *config_only: the
 * directory is kept for its configuration files only (CONFIG_ONLY_DIRS). */
static bool cif_directory_reason(cif_context *c, size_t parent, cbm_inventory_path_t path,
                                 cbm_inventory_filter_reason_t *reason, bool *config_only) {
    const char *name = cif_basename(c->owner, parent, path);
    if (!cif_poll(c)) {
        return false;
    }
    bool venv = false;
    if (cbm_discover_venv_candidate(name) && !cif_venv(c, path, &venv)) {
        return false;
    }
    bool safety_name = cbm_discover_is_safety_core_directory(name);
    bool named = cbm_should_skip_dir(name, CBM_MODE_FULL) || venv;
    bool config = !named && cbm_discover_config_only_dir(name);
    bool package = (named || config) && !safety_name &&
                   cbm_discover_path_in_jvm_package((const char *)path.data);
    bool builtin = named && !package;
    config = config && !package;
    bool suffix = !builtin && cbm_discover_path_has_skip_suffix((const char *)path.data);
    bool safety = (builtin || suffix) && c->owner->cbm_program && safety_name;
    if (!cif_poll(c)) {
        return false;
    }
    if (builtin || suffix || config) {
        cbm_ignore_decision_t decision = CBM_IGNORE_NO_OPINION;
        if (!safety && !cif_match(c, c->owner->cbm_program, path, true, &decision)) {
            return false;
        }
        if (decision != CBM_IGNORE_REINCLUDED && !config) {
            *reason = builtin ? CBM_INVENTORY_FILTER_REASON_DIRECTORY_BUILTIN
                              : CBM_INVENTORY_FILTER_REASON_DIRECTORY_SUFFIX;
            return true;
        }
        *config_only = decision != CBM_IGNORE_REINCLUDED;
    }
    return cif_ignore_reason(c, parent, path, true, reason);
}

static bool cif_file_entry(cif_context *c, size_t parent, size_t file) {
    cbm_inventory_filter_t *o = c->owner;
    c->file_index = file;
    cbm_inventory_filter_reason_t reason = CBM_INVENTORY_FILTER_REASON_NONE;
    if (!cif_file_reason(c, parent, o->files[file].path, &reason) || !cif_bytes(c, 128)) {
        return false;
    }
    o->rows[file].reason = reason;
    o->rows[file].config_only = o->directories[parent].config_only;
    if (reason != CBM_INVENTORY_FILTER_REASON_NONE) {
        o->rows[file].disposition = CBM_INVENTORY_FILTER_IGNORED_FILE;
    }
    return true;
}

static bool cif_directory_entry(cif_context *c, size_t parent, size_t directory) {
    cbm_inventory_filter_t *o = c->owner;
    c->file_index = SIZE_MAX;
    cbm_inventory_filter_reason_t reason = CBM_INVENTORY_FILTER_REASON_NONE;
    bool config_only = false;
    if (!cif_directory_reason(c, parent, o->directories[directory].path, &reason, &config_only) ||
        !cif_bytes(c, 128)) {
        return false;
    }
    o->directories[directory].reason = reason;
    /* Parents are filtered first, so the inherited flag is final here. */
    o->directories[directory].config_only = o->directories[parent].config_only || config_only;
    if (reason != CBM_INVENTORY_FILTER_REASON_NONE) {
        o->directories[directory].excluded = directory;
    }
    return true;
}

static bool cif_entries(cif_context *c, size_t parent) {
    cbm_inventory_filter_t *o = c->owner;
    size_t file = o->directories[parent].first_file;
    size_t directory = o->directories[parent].first_directory;
    while (file != SIZE_MAX || directory != SIZE_MAX) {
        if (!cif_event(c) || !cif_bytes(c, 128)) {
            return false;
        }
        int order = directory == SIZE_MAX ? -1 : 1;
        if (file != SIZE_MAX && directory != SIZE_MAX &&
            !cif_compare(c, o->files[file].path, o->directories[directory].path, &order)) {
            return false;
        }
        if (order < 0) {
            if (!cif_file_entry(c, parent, file)) {
                return false;
            }
            file = o->file_next[file];
        } else {
            if (!cif_directory_entry(c, parent, directory)) {
                return false;
            }
            directory = o->directories[directory].next;
        }
    }
    return true;
}

static bool cif_excluded_files(cif_context *c, size_t directory) {
    cbm_inventory_filter_t *o = c->owner;
    const cif_directory *d = &o->directories[directory];
    const cif_directory *excluded = &o->directories[d->excluded];
    for (size_t file = d->first_file; file != SIZE_MAX; file = o->file_next[file]) {
        c->file_index = file;
        if (!cif_event(c) || !cif_bytes(c, 256)) {
            return false;
        }
        o->rows[file].disposition = CBM_INVENTORY_FILTER_EXCLUDED_SUBTREE;
        o->rows[file].reason = excluded->reason;
        o->rows[file].excluded_ancestor = excluded->path;
    }
    return true;
}

static bool cif_final_ledgers(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    c->file_index = SIZE_MAX;
    /* Git rows were appended in canonical directory order; CBM kind follows. */
    if (!cif_copy(c, &o->controls[o->view.control_count], &o->cbm_control,
                  sizeof(o->cbm_control))) {
        return false;
    }
    o->view.control_count++;
    for (size_t i = 1; i < o->view.directory_count; i++) {
        if (!cif_event(c) || !cif_bytes(c, 128)) {
            return false;
        }
        if (o->directories[i].excluded == i) {
            o->exclusions[o->view.excluded_count++] = o->directories[i].path;
        }
    }
    return true;
}

bool cif_filter(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    for (size_t i = 0; i < o->view.directory_count; i++) {
        c->file_index = SIZE_MAX;
        if (!cif_event(c) || !cif_bytes(c, 256)) {
            return false;
        }
        cif_directory *d = &o->directories[i];
        if (d->parent != SIZE_MAX && o->directories[d->parent].excluded != SIZE_MAX) {
            d->excluded = o->directories[d->parent].excluded;
        }
        if (d->excluded != SIZE_MAX) {
            if (!cif_excluded_files(c, i)) {
                return false;
            }
        } else if (!cif_load_controls(c, i) || !cif_entries(c, i)) {
            return false;
        }
    }
    return cif_final_ledgers(c);
}
