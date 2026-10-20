/*
 * cbm_fuzz.c — coverage-guided fuzzing of the parsers that read untrusted bytes.
 *
 * One libFuzzer binary, three targets, chosen by CBM_FUZZ_TARGET:
 *
 *   extract   "<file name>\n<source>": the language comes from the file name
 *             exactly as discovery would pick it, the source goes through the
 *             full per-file extraction (tree-sitter grammar, extractors, LSP
 *             passes) and the result is freed. Every grammar and extractor is
 *             reachable from one corpus.
 *   cypher    the whole input is a query string for the lexer and parser.
 *   config    "<t|y|j><document>": the document is written to a private file
 *             and one managed-entry upsert plus remove runs on it through the
 *             TOML, YAML or JSON editor -- the code that rewrites a user's own
 *             agent configuration.
 *
 * Built by `make -f Makefile.cbm fuzz` (clang with -fsanitize=fuzzer on top of
 * ASan+UBSan; every vendored object is compiled with fuzzer-no-link, so the
 * grammars feed coverage back too). Driven by scripts/fuzz.sh, which seeds each
 * target from tests/fuzz/corpus/<target>/.
 */
#include "../../internal/cbm/cbm.h"
#include "../../src/cli/config_json_like.h"
#include "../../src/cli/config_toml_edit.h"
#include "../../src/cli/config_yaml_edit.h"
#include "../../src/cypher/cypher.h"
#include "../../src/discover/discover.h"
#include "../../src/foundation/compat_fs.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

enum { FUZZ_EXTRACT, FUZZ_CYPHER, FUZZ_CONFIG };
enum {
    FUZZ_NAME_MAX = 128,
    FUZZ_EXTRACT_BUDGET_MICROS = 2000000, /* a hang is a finding; a slow parse is not */
    FUZZ_PATH_MAX = 512,
    FUZZ_EXIT_SETUP = 2, /* the harness itself could not start */
    FUZZ_NUL = 1,        /* room for a terminator; also the newline after the name */
    FUZZ_JSON_PATH_LEN = 1,
    FUZZ_FILE_NAME_ROOM = 32, /* "/config.toml" and a terminator */
};

static int g_target = FUZZ_EXTRACT;
static char g_dir[FUZZ_PATH_MAX];

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc;
    (void)argv;
    const char *t = getenv("CBM_FUZZ_TARGET");
    if (t && strcmp(t, "cypher") == 0) {
        g_target = FUZZ_CYPHER;
    } else if (t && strcmp(t, "config") == 0) {
        g_target = FUZZ_CONFIG;
    } else if (t && strcmp(t, "extract") != 0) {
        (void)fprintf(stderr, "cbm_fuzz: CBM_FUZZ_TARGET must be extract, cypher or config\n");
        exit(FUZZ_EXIT_SETUP);
    }
    if (g_target == FUZZ_EXTRACT && cbm_init() != 0) {
        (void)fprintf(stderr, "cbm_fuzz: cbm_init failed\n");
        exit(FUZZ_EXIT_SETUP);
    }
    if (g_target == FUZZ_CONFIG) {
        const char *tmp = getenv("TMPDIR");
        (void)snprintf(g_dir, sizeof(g_dir), "%s/cbm-fuzz-XXXXXX", tmp && tmp[0] ? tmp : "/tmp");
        if (!mkdtemp(g_dir)) {
            perror("cbm_fuzz: mkdtemp");
            exit(FUZZ_EXIT_SETUP);
        }
    }
    return 0;
}

static void fuzz_extract(const uint8_t *data, size_t size) {
    size_t scan = size < FUZZ_NAME_MAX ? size : FUZZ_NAME_MAX;
    const uint8_t *nl = memchr(data, '\n', scan);
    if (!nl || nl == data) {
        return;
    }
    size_t name_len = (size_t)(nl - data);
    char name[FUZZ_NAME_MAX + FUZZ_NUL];
    memcpy(name, data, name_len);
    name[name_len] = '\0';
    CBMLanguage lang = cbm_language_for_filename(name);
    if ((int)lang < 0 || lang >= CBM_LANG_COUNT) {
        return;
    }
    size_t src_len = size - name_len - FUZZ_NUL;
    if (src_len > (size_t)INT32_MAX) {
        return;
    }
    /* The pipeline hands extraction a file buffer with a terminator after
     * source_len bytes, and parts of the LSP passes rely on it (strlen over the
     * source). Fuzz the contract production honours, not a stricter one. */
    char *source = malloc(src_len + FUZZ_NUL);
    if (!source) {
        return;
    }
    memcpy(source, nl + FUZZ_NUL, src_len);
    source[src_len] = '\0';
    CBMFileResult *r = cbm_extract_file(source, (int)src_len, lang, "fuzz", name,
                                        FUZZ_EXTRACT_BUDGET_MICROS, NULL, NULL);
    if (r) {
        cbm_free_tree(r);
        cbm_free_result(r);
    }
    free(source);
}

static void fuzz_cypher(const uint8_t *data, size_t size) {
    char *query = malloc(size + FUZZ_NUL);
    if (!query) {
        return;
    }
    memcpy(query, data, size);
    query[size] = '\0';
    cbm_query_t *q = NULL;
    char *error = NULL;
    (void)cbm_cypher_parse(query, &q, &error);
    cbm_query_free(q);
    free(error);
    free(query);
}

static void fuzz_config(const uint8_t *data, size_t size) {
    if (size < FUZZ_NUL) {
        return;
    }
    char path[FUZZ_PATH_MAX + FUZZ_FILE_NAME_ROOM];
    const char *ext = data[0] == 't' ? "toml" : data[0] == 'y' ? "yaml" : "json";
    (void)snprintf(path, sizeof(path), "%s/config.%s", g_dir, ext);
    FILE *f = cbm_fopen(path, "wb");
    if (!f) {
        return;
    }
    size_t body = size - FUZZ_NUL;
    if (body > 0 && fwrite(data + FUZZ_NUL, FUZZ_NUL, body, f) != body) {
        (void)fclose(f);
        return;
    }
    if (fclose(f) != 0) {
        return;
    }
    switch (data[0]) {
    case 't':
        (void)cbm_toml_upsert_managed_block(path, "# BEGIN codebase-memory",
                                            "# END codebase-memory", "owned = true\n");
        (void)cbm_toml_remove_managed_block(path, "# BEGIN codebase-memory",
                                            "# END codebase-memory");
        break;
    case 'y':
        (void)cbm_yaml_upsert_mapping_entry(path, "mcp_servers", "codebase-memory",
                                            "    command: codebase-memory-mcp\n");
        (void)cbm_yaml_remove_mapping_entry(path, "mcp_servers", "codebase-memory");
        break;
    default: {
        const char *object_path[] = {"mcpServers"};
        (void)cbm_json_like_upsert_entry(path, object_path, FUZZ_JSON_PATH_LEN, "codebase-memory",
                                         "{\"command\":\"codebase-memory-mcp\"}");
        char *content = NULL;
        size_t length = 0;
        if (cbm_json_like_read_document(path, &content, &length) == 0) {
            free(content);
        }
        break;
    }
    }
    (void)unlink(path);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    switch (g_target) {
    case FUZZ_CYPHER:
        fuzz_cypher(data, size);
        break;
    case FUZZ_CONFIG:
        fuzz_config(data, size);
        break;
    default:
        fuzz_extract(data, size);
        break;
    }
    return 0;
}
