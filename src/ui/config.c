/*
 * config.c — Persistent UI configuration (JSON via yyjson).
 *
 * Config file: ~/.cache/codebase-memory-mcp/config.json
 * Format: {"ui_enabled": false, "ui_port": 9749}
 */
#include "ui/config.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/compat.h"

#include <yyjson/yyjson.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Path ────────────────────────────────────────────────────── */

void cbm_ui_config_path(char *buf, int bufsz) {
    const char *home = cbm_get_home_dir();
    if (!home) {
        home = cbm_tmpdir();
    }
    snprintf(buf, (size_t)bufsz, "%s/.cache/codebase-memory-mcp/config.json", home);
}

/* ── Load ────────────────────────────────────────────────────── */

void cbm_ui_config_load(cbm_ui_config_t *cfg) {
    cfg->ui_enabled = CBM_UI_DEFAULT_ENABLED;
    cfg->ui_port = CBM_UI_DEFAULT_PORT;

    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) {
        return; /* no config file → defaults */
    }

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (len <= 0 || len > 4096) {
        fclose(f);
        return; /* empty or suspiciously large → defaults */
    }

    char *buf = malloc((size_t)len + 1);
    if (!buf) {
        fclose(f);
        return;
    }

    size_t nread = fread(buf, 1, (size_t)len, f);
    fclose(f);
    buf[nread] = '\0';

    yyjson_doc *doc = yyjson_read(buf, nread, 0);
    free(buf);
    if (!doc) {
        cbm_log_warn("ui.config.corrupt", "path", path);
        return; /* corrupt JSON → defaults */
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return;
    }

    yyjson_val *v_enabled = yyjson_obj_get(root, "ui_enabled");
    if (yyjson_is_bool(v_enabled)) {
        cfg->ui_enabled = yyjson_get_bool(v_enabled);
    }

    yyjson_val *v_port = yyjson_obj_get(root, "ui_port");
    if (yyjson_is_int(v_port)) {
        cfg->ui_port = (int)yyjson_get_int(v_port);
    }

    yyjson_doc_free(doc);
}

/* ── Save ────────────────────────────────────────────────────── */

void cbm_ui_config_save(const cbm_ui_config_t *cfg) {
    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    /* Ensure directory exists (recursive) */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        cbm_mkdir_p(dir, 0750);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_bool(doc, root, "ui_enabled", cfg->ui_enabled);
    yyjson_mut_obj_add_int(doc, root, "ui_port", cfg->ui_port);

    size_t json_len = 0;
    char *json = yyjson_mut_write(doc, YYJSON_WRITE_PRETTY, &json_len);
    yyjson_mut_doc_free(doc);

    if (!json) {
        cbm_log_error("ui.config.write_fail", "reason", "serialize");
        return;
    }

    FILE *f = fopen(path, "wb");
    if (!f) {
        cbm_log_error("ui.config.write_fail", "path", path);
        free(json);
        return;
    }

    fwrite(json, 1, json_len, f);
    fclose(f);
    free(json);

    cbm_log_debug("ui.config.saved", "path", path);
}
