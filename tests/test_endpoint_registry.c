/*
 * test_endpoint_registry.c — Tests for cross-repo endpoint registry types and helpers.
 *
 * Tests cover:
 *   - Endpoint list creation and free (including NULL-safety)
 *   - Registering endpoints and verifying all fields
 *   - Auto-growing beyond initial capacity
 *   - Skipping empty/NULL identifiers
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include <pipeline/servicelink.h>
#include <string.h>

/* ── Tests ──────────────────────────────────────────────────────── */

TEST(endpoint_list_create_and_free) {
    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    ASSERT_NOT_NULL(list);
    ASSERT_EQ(list->count, 0);
    ASSERT_EQ(list->capacity, SL_ENDPOINT_INITIAL_CAP);
    cbm_sl_endpoint_list_free(list);
    /* Free NULL should not crash */
    cbm_sl_endpoint_list_free(NULL);
    PASS();
}

TEST(endpoint_list_register_and_count) {
    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    ASSERT_NOT_NULL(list);

    sl_register_endpoint(list, "myproject", "graphql", "producer",
                         "getUser", "resolvers.UserResolver.getUser",
                         "src/resolvers/user.ts", "{\"kind\":\"query\"}");

    sl_register_endpoint(list, "myproject", "graphql", "consumer",
                         "getUser", "hooks.useGetUser",
                         "src/hooks/user.ts", "");

    sl_register_endpoint(list, "myproject", "kafka", "producer",
                         "user.created", "services.UserService.create",
                         "src/services/user.ts", "{\"topic\":\"user.created\"}");

    ASSERT_EQ(list->count, 3);

    /* Verify first endpoint fields */
    ASSERT_STR_EQ(list->items[0].project, "myproject");
    ASSERT_STR_EQ(list->items[0].protocol, "graphql");
    ASSERT_STR_EQ(list->items[0].role, "producer");
    ASSERT_STR_EQ(list->items[0].identifier, "getUser");
    ASSERT_STR_EQ(list->items[0].node_qn, "resolvers.UserResolver.getUser");
    ASSERT_STR_EQ(list->items[0].file_path, "src/resolvers/user.ts");
    ASSERT_STR_EQ(list->items[0].extra, "{\"kind\":\"query\"}");

    /* Verify second endpoint */
    ASSERT_STR_EQ(list->items[1].role, "consumer");
    ASSERT_STR_EQ(list->items[1].node_qn, "hooks.useGetUser");

    /* Verify third endpoint */
    ASSERT_STR_EQ(list->items[2].protocol, "kafka");
    ASSERT_STR_EQ(list->items[2].identifier, "user.created");

    cbm_sl_endpoint_list_free(list);
    PASS();
}

TEST(endpoint_list_grows_beyond_initial_capacity) {
    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    ASSERT_NOT_NULL(list);

    /* Register more than SL_ENDPOINT_INITIAL_CAP (256) endpoints */
    for (int i = 0; i < 300; i++) {
        char ident[64];
        snprintf(ident, sizeof(ident), "topic_%d", i);
        sl_register_endpoint(list, "proj", "kafka", "producer",
                             ident, "fn", "file.ts", "");
    }

    ASSERT_EQ(list->count, 300);
    ASSERT_GTE(list->capacity, 300);

    /* Verify first and last entries survived realloc */
    ASSERT_STR_EQ(list->items[0].identifier, "topic_0");
    ASSERT_STR_EQ(list->items[299].identifier, "topic_299");

    cbm_sl_endpoint_list_free(list);
    PASS();
}

TEST(endpoint_list_skips_empty_identifier) {
    cbm_sl_endpoint_list_t *list = cbm_sl_endpoint_list_new();
    ASSERT_NOT_NULL(list);

    /* Empty string identifier should be skipped */
    sl_register_endpoint(list, "proj", "kafka", "producer",
                         "", "fn", "file.ts", "");
    ASSERT_EQ(list->count, 0);

    /* NULL identifier should be skipped */
    sl_register_endpoint(list, "proj", "kafka", "producer",
                         NULL, "fn", "file.ts", "");
    ASSERT_EQ(list->count, 0);

    /* NULL list should not crash */
    sl_register_endpoint(NULL, "p", "proto", "role", "id", "qn", "f", "e");

    cbm_sl_endpoint_list_free(list);
    PASS();
}

SUITE(endpoint_registry) {
    RUN_TEST(endpoint_list_create_and_free);
    RUN_TEST(endpoint_list_register_and_count);
    RUN_TEST(endpoint_list_grows_beyond_initial_capacity);
    RUN_TEST(endpoint_list_skips_empty_identifier);
}
