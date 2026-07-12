/*
 * config.c — Persistent UI configuration (JSON via yyjson).
 *
 * Config file: ~/.cache/codebase-memory-mcp/config.json
 * Format: {"ui_enabled": false, "ui_port": 9749, "ui_host": "127.0.0.1"}
 */
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdatomic.h>
#endif

#include "foundation/constants.h"
#include "ui/config.h"
#include "ui/embedded_assets.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/compat.h"

#include <yyjson/yyjson.h>

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <arpa/inet.h>
#endif

#ifdef _WIN32
static void cbm_ui_host_socket_init(void) {
    static atomic_int started = 0;
    int expected = 0;
    if (atomic_compare_exchange_strong(&started, &expected, 1)) {
        WSADATA wsa;
        (void)WSAStartup(MAKEWORD(2, 2), &wsa);
    }
}
#endif

bool cbm_ui_host_is_valid(const char *host) {
    struct in_addr addr;
#ifdef _WIN32
    cbm_ui_host_socket_init();
#endif
    return host && host[0] != '\0' && inet_pton(AF_INET, host, &addr) == 1;
}

bool cbm_ui_host_is_loopback(const char *host) {
    struct in_addr addr;
#ifdef _WIN32
    cbm_ui_host_socket_init();
#endif
    if (!host || inet_pton(AF_INET, host, &addr) != 1)
        return false;
    return (ntohl(addr.s_addr) & UINT32_C(0xff000000)) == UINT32_C(0x7f000000);
}

/* ── Path ────────────────────────────────────────────────────── */

void cbm_ui_config_path(char *buf, int bufsz) {
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) {
        dir = cbm_tmpdir();
    }
    snprintf(buf, (size_t)bufsz, "%s/config.json", dir);
}

/* ── Load ────────────────────────────────────────────────────── */

void cbm_ui_config_load(cbm_ui_config_t *cfg) {
    cfg->ui_enabled = CBM_UI_DEFAULT_ENABLED;
    cfg->ui_port = CBM_UI_DEFAULT_PORT;
    snprintf(cfg->ui_host, sizeof(cfg->ui_host), "%s", CBM_UI_DEFAULT_HOST);

    char path[CBM_SZ_1K];
    cbm_ui_config_path(path, (int)sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) {
        /* No config file — auto-enable UI if binary has embedded assets */
        if (CBM_EMBEDDED_FILE_COUNT > 0) {
            cfg->ui_enabled = true;
        }
        return;
    }

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (len <= 0 || len > 4096) {
        fclose(f);
        return; /* empty or suspiciously large → defaults */
    }

    char *buf = malloc((size_t)len + SKIP_ONE);
    if (!buf) {
        fclose(f);
        return;
    }

    size_t nread = fread(buf, SKIP_ONE, (size_t)len, f);
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

    yyjson_val *v_host = yyjson_obj_get(root, "ui_host");
    if (yyjson_is_str(v_host)) {
        const char *host = yyjson_get_str(v_host);
        if (cbm_ui_host_is_valid(host)) {
            snprintf(cfg->ui_host, sizeof(cfg->ui_host), "%s", host);
        }
    }

    yyjson_doc_free(doc);
}

/* ── Save ────────────────────────────────────────────────────── */

void cbm_ui_config_save(const cbm_ui_config_t *cfg) {
    char path[CBM_SZ_1K];
    cbm_ui_config_path(path, (int)sizeof(path));

    /* Ensure directory exists (recursive) */
    char dir[CBM_SZ_1K];
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
    yyjson_mut_obj_add_str(doc, root, "ui_host",
                           cbm_ui_host_is_valid(cfg->ui_host) ? cfg->ui_host : CBM_UI_DEFAULT_HOST);

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

    fwrite(json, SKIP_ONE, json_len, f);
    fclose(f);
    free(json);

    cbm_log_debug("ui.config.saved", "path", path);
}
