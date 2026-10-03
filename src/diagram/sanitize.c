/*
 * sanitize.c — Identifier and label escaping for Mermaid and Graphviz DOT.
 */
#include "diagram/diagram.h"
#include <ctype.h>
#include <stdio.h>
#include <string.h>

void cbm_diagram_sanitize_id(const char *raw, char *buf, size_t cap) {
    if (!buf || cap == 0) return;
    buf[0] = '\0';
    if (!raw || raw[0] == '\0') {
        snprintf(buf, cap, "node");
        return;
    }

    size_t out_idx = 0;
    /* If starts with a digit, prefix with n_ */
    if (isdigit((unsigned char)raw[0])) {
        if (cap > 2) {
            buf[out_idx++] = 'n';
            buf[out_idx++] = '_';
        }
    }

    for (size_t i = 0; raw[i] != '\0' && out_idx + 1 < cap; i++) {
        unsigned char c = (unsigned char)raw[i];
        if (isalnum(c) || c == '_') {
            buf[out_idx++] = (char)c;
        } else {
            buf[out_idx++] = '_';
        }
    }
    buf[out_idx] = '\0';
}

void cbm_diagram_sanitize_label(const char *raw, char *buf, size_t cap) {
    if (!buf || cap == 0) return;
    buf[0] = '\0';
    if (!raw || raw[0] == '\0') {
        return;
    }

    size_t out_idx = 0;
    for (size_t i = 0; raw[i] != '\0' && out_idx + 2 < cap; i++) {
        unsigned char c = (unsigned char)raw[i];
        if (c == '"') {
            if (out_idx + 2 < cap) {
                buf[out_idx++] = '\\';
                buf[out_idx++] = '"';
            }
        } else if (c == '\\') {
            if (out_idx + 2 < cap) {
                buf[out_idx++] = '\\';
                buf[out_idx++] = '\\';
            }
        } else if (c == '\n' || c == '\r') {
            buf[out_idx++] = ' ';
        } else if (c >= 32 && c < 127) {
            buf[out_idx++] = (char)c;
        } else {
            buf[out_idx++] = ' ';
        }
    }
    buf[out_idx] = '\0';
}
