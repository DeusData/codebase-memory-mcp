#include "test_framework.h"
#include "foundation/mem.h"
#include <mcp/mcp.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;

extern void suite_diagram(void);

int main(int argc, char **argv) {
    cbm_mem_init(1024 * 1024 * 128);

    if (argc > 1 && strcmp(argv[1], "--test") != 0) {
        const char *type = argv[1];
        const char *proj = (argc > 2) ? argv[2] : "codebase-memory-mcp-ui";
        const char *entry = (argc > 3) ? argv[3] : "";
        const char *format = (argc > 4) ? argv[4] : "mermaid";

        cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
        if (!srv) {
            fprintf(stderr, "Failed to initialize MCP server\n");
            return 1;
        }

        char args[1024];
        if (entry && entry[0] != '\0') {
            snprintf(args, sizeof(args),
                     "{\"type\":\"%s\",\"project\":\"%s\",\"entry_point\":\"%s\",\"format\":\"%s\"}",
                     type, proj, entry, format);
        } else {
            snprintf(args, sizeof(args),
                     "{\"type\":\"%s\",\"project\":\"%s\",\"format\":\"%s\"}",
                     type, proj, format);
        }

        char *result_json = cbm_mcp_handle_tool(srv, "export_diagram", args);
        if (result_json) {
            printf("%s\n", result_json);
            free(result_json);
        } else {
            fprintf(stderr, "export_diagram returned NULL\n");
        }

        cbm_mcp_server_free(srv);
        return 0;
    }

    printf("=== Native Diagram Generation Test Runner ===\n\n");
    RUN_SUITE(diagram);
    TEST_SUMMARY();
    return tf_fail_count > 0 ? 1 : 0;
}
