/* Generated from product-manifest.json. Run scripts/generate-product-surfaces.py. */
#ifndef CBM_PRODUCT_MANIFEST_GENERATED_H
#define CBM_PRODUCT_MANIFEST_GENERATED_H

#define CBM_PRODUCT_NAME "codebase-memory-mcp"
#define CBM_PRODUCT_VERSION "0.8.1"
#define CBM_PRODUCT_STATUS "preview"

#define CBM_PRODUCT_CORE_TOOL_COUNT 9
#define CBM_PRODUCT_ADVANCED_TOOL_COUNT 3
#define CBM_PRODUCT_ADMIN_TOOL_COUNT 2

#define CBM_PRODUCT_CORE_TOOLS(APPLY) \
    APPLY("get_context") \
    APPLY("search_graph") \
    APPLY("search_code") \
    APPLY("trace_path") \
    APPLY("get_code_snippet") \
    APPLY("get_architecture") \
    APPLY("detect_changes") \
    APPLY("index_status") \
    APPLY("index_repository")

#define CBM_PRODUCT_ADVANCED_TOOLS(APPLY) \
    APPLY("query_graph") \
    APPLY("get_graph_schema") \
    APPLY("manage_adr")

#define CBM_PRODUCT_ADMIN_TOOLS(APPLY) \
    APPLY("list_projects") \
    APPLY("delete_project")

#endif
