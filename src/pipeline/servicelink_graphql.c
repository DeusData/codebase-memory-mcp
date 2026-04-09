/*
 * servicelink_graphql.c — GraphQL protocol linker for cross-service linking.
 *
 * Discovers GraphQL producers (SDL definitions, resolvers) and consumers
 * (client queries/mutations via useQuery, gql`...`, client.execute, etc.)
 * and creates GRAPHQL_CALLS edges between them.
 *
 * Languages: JavaScript/TypeScript, Python, Go, Java/Kotlin, Ruby, PHP
 */

#include "servicelink.h"
#include "foundation/compat.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define CONF_EXACT_MATCH      0.95
#define CONF_NORMALIZED_MATCH 0.85
#define CONF_FUZZY_MATCH      0.65
#define FUZZY_THRESHOLD       0.85

/* ── itoa helper for logging ───────────────────────────────────── */

static const char *itoa_gql(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Name normalization ────────────────────────────────────────── */

/*
 * Normalize a name to lowercase with no underscores.
 * "getUser" -> "getuser", "get_user" -> "getuser", "GetUser" -> "getuser"
 */
static void normalize_name(const char *in, char *out, int out_size) {
    int j = 0;
    for (int i = 0; in[i] && j < out_size - 1; i++) {
        if (in[i] == '_') {
            continue;
        }
        out[j++] = (char)tolower((unsigned char)in[i]);
    }
    out[j] = '\0';
}

/* ── SDL scanning (file-level, .graphql/.gql files) ────────────── */

/*
 * Scan a .graphql or .gql file for type definitions.
 * Extracts field names from Query, Mutation, Subscription types.
 * Each field becomes a producer.
 */
static int scan_sdl_file(const cbm_pipeline_ctx_t *ctx,
                         const cbm_gbuf_node_t *node,
                         const char *source,
                         cbm_sl_producer_t *prods, int max_prods) {
    int count = 0;

    /* Pattern: type (Query|Mutation|Subscription) { ... }
     * Extract field names from the block. */
    cbm_regex_t type_re;
    if (cbm_regcomp(&type_re,
                     "type[[:space:]]+(Query|Mutation|Subscription)[[:space:]]*\\{",
                     CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    /* Field name pattern: word at start of line (after whitespace) followed by
     * optional args and colon — e.g. "  getUser(id: ID!): User" */
    cbm_regex_t field_re;
    if (cbm_regcomp(&field_re,
                     "^[[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*)[[:space:]]*[\\(:]",
                     CBM_REG_EXTENDED | CBM_REG_NEWLINE) != 0) {
        cbm_regfree(&type_re);
        return 0;
    }

    const char *p = source;
    cbm_regmatch_t tm[2];

    while (count < max_prods && cbm_regexec(&type_re, p, 2, tm, 0) == 0) {
        /* Extract type kind (Query/Mutation/Subscription) */
        char kind[32] = {0};
        int klen = tm[1].rm_eo - tm[1].rm_so;
        if (klen > (int)sizeof(kind) - 1) {
            klen = (int)sizeof(kind) - 1;
        }
        memcpy(kind, p + tm[1].rm_so, (size_t)klen);
        kind[klen] = '\0';

        /* Lowercase the kind for the extra field */
        for (int i = 0; kind[i]; i++) {
            kind[i] = (char)tolower((unsigned char)kind[i]);
        }

        /* Find the matching closing brace */
        const char *block_start = p + tm[0].rm_eo;
        int depth = 1;
        const char *block_end = block_start;
        while (*block_end && depth > 0) {
            if (*block_end == '{') {
                depth++;
            } else if (*block_end == '}') {
                depth--;
            }
            if (depth > 0) {
                block_end++;
            }
        }

        /* Extract field names from within this block */
        /* We scan line by line to avoid nested type fields */
        const char *line = block_start;
        while (line < block_end && count < max_prods) {
            /* Find end of line */
            const char *eol = line;
            while (eol < block_end && *eol != '\n') {
                eol++;
            }

            /* Check this line for a field definition */
            int line_len = (int)(eol - line);
            char line_buf[512];
            if (line_len > (int)sizeof(line_buf) - 1) {
                line_len = (int)sizeof(line_buf) - 1;
            }
            memcpy(line_buf, line, (size_t)line_len);
            line_buf[line_len] = '\0';

            /* Skip comments and nested type blocks */
            const char *trimmed = line_buf;
            while (*trimmed == ' ' || *trimmed == '\t') {
                trimmed++;
            }
            if (*trimmed != '#' && *trimmed != '}' && *trimmed != '{') {
                cbm_regmatch_t fm[2];
                if (cbm_regexec(&field_re, line_buf, 2, fm, 0) == 0) {
                    cbm_sl_producer_t *prod = &prods[count];
                    int flen = fm[1].rm_eo - fm[1].rm_so;
                    if (flen > (int)sizeof(prod->identifier) - 1) {
                        flen = (int)sizeof(prod->identifier) - 1;
                    }
                    memcpy(prod->identifier, line_buf + fm[1].rm_so, (size_t)flen);
                    prod->identifier[flen] = '\0';
                    snprintf(prod->source_qn, sizeof(prod->source_qn), "%s",
                             node->qualified_name);
                    prod->source_id = node->id;
                    snprintf(prod->file_path, sizeof(prod->file_path), "%s",
                             node->file_path);
                    snprintf(prod->extra, sizeof(prod->extra), "%s", kind);
                    count++;
                }
            }

            line = eol;
            if (*line == '\n') {
                line++;
            }
        }

        p = block_end;
        if (*p == '}') {
            p++;
        }
    }

    cbm_regfree(&type_re);
    cbm_regfree(&field_re);

    (void)ctx;
    return count;
}

/* ── Resolver detection (code files) ───────────────────────────── */

/*
 * Detect resolver patterns in source code and add as producers.
 * Patterns:
 *   - @Query() / @Mutation() / @Resolver() decorators (NestJS/TypeGraphQL)
 *   - resolvers: { Query: { fieldName: ... } } (Apollo Server)
 *   - func (r *queryResolver) FieldName(...) (Go gqlgen)
 */
static int scan_resolvers(const cbm_pipeline_ctx_t *ctx,
                          const cbm_gbuf_node_t *node,
                          const char *source,
                          cbm_sl_producer_t *prods, int max_prods) {
    int count = 0;
    (void)ctx;

    /* Pattern 1: @Query('name') or @Query() with method name */
    cbm_regex_t decorator_re;
    if (cbm_regcomp(&decorator_re,
                     "@(Query|Mutation|Subscription)\\([[:space:]]*['\"]?([a-zA-Z_][a-zA-Z0-9_]*)?['\"]?",
                     CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    /* Pattern 2: Go gqlgen resolver: func (r *queryResolver) FieldName */
    cbm_regex_t go_resolver_re;
    if (cbm_regcomp(&go_resolver_re,
                     "func[[:space:]]+\\([a-zA-Z_]+[[:space:]]+\\*?(query|mutation|subscription)Resolver\\)[[:space:]]+([A-Z][a-zA-Z0-9_]*)",
                     CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&decorator_re);
        return 0;
    }

    /* Pattern 3: resolvers object: Query: { fieldName: (parent, args) => ... } */
    cbm_regex_t resolver_obj_re;
    if (cbm_regcomp(&resolver_obj_re,
                     "(Query|Mutation|Subscription)[[:space:]]*:[[:space:]]*\\{",
                     CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&decorator_re);
        cbm_regfree(&go_resolver_re);
        return 0;
    }

    /* Pattern 4: field within resolver object: fieldName: */
    cbm_regex_t resolver_field_re;
    if (cbm_regcomp(&resolver_field_re,
                     "^[[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*)[[:space:]]*:",
                     CBM_REG_EXTENDED | CBM_REG_NEWLINE) != 0) {
        cbm_regfree(&decorator_re);
        cbm_regfree(&go_resolver_re);
        cbm_regfree(&resolver_obj_re);
        return 0;
    }

    const char *p = source;
    cbm_regmatch_t dm[3];

    /* Scan for decorator-style resolvers */
    while (count < max_prods && cbm_regexec(&decorator_re, p, 3, dm, 0) == 0) {
        cbm_sl_producer_t *prod = &prods[count];

        /* Extract the kind (Query/Mutation/Subscription) */
        char kind[32] = {0};
        int klen = dm[1].rm_eo - dm[1].rm_so;
        if (klen > (int)sizeof(kind) - 1) {
            klen = (int)sizeof(kind) - 1;
        }
        memcpy(kind, p + dm[1].rm_so, (size_t)klen);
        kind[klen] = '\0';

        /* Extract explicit name if provided, otherwise use the node name */
        if (dm[2].rm_so >= 0 && dm[2].rm_eo > dm[2].rm_so) {
            int nlen = dm[2].rm_eo - dm[2].rm_so;
            if (nlen > (int)sizeof(prod->identifier) - 1) {
                nlen = (int)sizeof(prod->identifier) - 1;
            }
            memcpy(prod->identifier, p + dm[2].rm_so, (size_t)nlen);
            prod->identifier[nlen] = '\0';
        } else {
            snprintf(prod->identifier, sizeof(prod->identifier), "%s",
                     node->name);
        }

        snprintf(prod->source_qn, sizeof(prod->source_qn), "%s",
                 node->qualified_name);
        prod->source_id = node->id;
        snprintf(prod->file_path, sizeof(prod->file_path), "%s",
                 node->file_path);
        /* Lowercase kind for extra */
        for (int i = 0; kind[i]; i++) {
            kind[i] = (char)tolower((unsigned char)kind[i]);
        }
        snprintf(prod->extra, sizeof(prod->extra), "%s", kind);
        count++;

        p += dm[0].rm_eo;
    }

    /* Scan for Go gqlgen resolvers */
    p = source;
    while (count < max_prods && cbm_regexec(&go_resolver_re, p, 3, dm, 0) == 0) {
        cbm_sl_producer_t *prod = &prods[count];

        /* Extract kind */
        char kind[32] = {0};
        int klen = dm[1].rm_eo - dm[1].rm_so;
        if (klen > (int)sizeof(kind) - 1) {
            klen = (int)sizeof(kind) - 1;
        }
        memcpy(kind, p + dm[1].rm_so, (size_t)klen);
        kind[klen] = '\0';

        /* Extract field name */
        int nlen = dm[2].rm_eo - dm[2].rm_so;
        if (nlen > (int)sizeof(prod->identifier) - 1) {
            nlen = (int)sizeof(prod->identifier) - 1;
        }
        memcpy(prod->identifier, p + dm[2].rm_so, (size_t)nlen);
        prod->identifier[nlen] = '\0';

        snprintf(prod->source_qn, sizeof(prod->source_qn), "%s",
                 node->qualified_name);
        prod->source_id = node->id;
        snprintf(prod->file_path, sizeof(prod->file_path), "%s",
                 node->file_path);
        snprintf(prod->extra, sizeof(prod->extra), "%s", kind);
        count++;

        p += dm[0].rm_eo;
    }

    /* Scan for resolver objects: resolvers: { Query: { field1: ..., field2: ... } } */
    p = source;
    cbm_regmatch_t rm[2];
    while (count < max_prods && cbm_regexec(&resolver_obj_re, p, 2, rm, 0) == 0) {
        char kind[32] = {0};
        int klen = rm[1].rm_eo - rm[1].rm_so;
        if (klen > (int)sizeof(kind) - 1) {
            klen = (int)sizeof(kind) - 1;
        }
        memcpy(kind, p + rm[1].rm_so, (size_t)klen);
        kind[klen] = '\0';
        for (int i = 0; kind[i]; i++) {
            kind[i] = (char)tolower((unsigned char)kind[i]);
        }

        /* Find the block */
        const char *block_start = p + rm[0].rm_eo;
        int depth = 1;
        const char *block_end = block_start;
        while (*block_end && depth > 0) {
            if (*block_end == '{') {
                depth++;
            } else if (*block_end == '}') {
                depth--;
            }
            if (depth > 0) {
                block_end++;
            }
        }

        /* Extract field names from within this resolver block */
        const char *line = block_start;
        while (line < block_end && count < max_prods) {
            const char *eol = line;
            while (eol < block_end && *eol != '\n') {
                eol++;
            }

            int line_len = (int)(eol - line);
            char line_buf[512];
            if (line_len > (int)sizeof(line_buf) - 1) {
                line_len = (int)sizeof(line_buf) - 1;
            }
            memcpy(line_buf, line, (size_t)line_len);
            line_buf[line_len] = '\0';

            cbm_regmatch_t fm[2];
            if (cbm_regexec(&resolver_field_re, line_buf, 2, fm, 0) == 0) {
                cbm_sl_producer_t *prod = &prods[count];
                int flen = fm[1].rm_eo - fm[1].rm_so;
                if (flen > (int)sizeof(prod->identifier) - 1) {
                    flen = (int)sizeof(prod->identifier) - 1;
                }
                memcpy(prod->identifier, line_buf + fm[1].rm_so, (size_t)flen);
                prod->identifier[flen] = '\0';
                snprintf(prod->source_qn, sizeof(prod->source_qn), "%s",
                         node->qualified_name);
                prod->source_id = node->id;
                snprintf(prod->file_path, sizeof(prod->file_path), "%s",
                         node->file_path);
                snprintf(prod->extra, sizeof(prod->extra), "%s", kind);
                count++;
            }

            line = eol;
            if (*line == '\n') {
                line++;
            }
        }

        p = block_end;
        if (*p == '}') {
            p++;
        }
    }

    cbm_regfree(&decorator_re);
    cbm_regfree(&go_resolver_re);
    cbm_regfree(&resolver_obj_re);
    cbm_regfree(&resolver_field_re);

    return count;
}

/* ── Field-name extraction ────────────────────────────────────── */

/*
 * Extract the first field name from a GraphQL operation body.
 * Given source starting at the operation line like:
 *   "query formatNotification($params: ...) {\n    formatMessage(params: ...) {\n"
 * Finds the first '{' then the first identifier after it.
 * Returns the field name in `out`, or empty string if not found.
 */
static void extract_first_field_name(const char *op_start, char *out, int out_size) {
    out[0] = '\0';
    /* Find the opening brace of the operation body */
    const char *brace = strchr(op_start, '{');
    if (!brace) return;
    brace++; /* skip past '{' */

    /* Skip whitespace (including newlines) */
    while (*brace && (*brace == ' ' || *brace == '\t' || *brace == '\n' || *brace == '\r')) {
        brace++;
    }

    /* Extract identifier: [a-zA-Z_][a-zA-Z0-9_]* */
    if (!((*brace >= 'a' && *brace <= 'z') || (*brace >= 'A' && *brace <= 'Z') || *brace == '_')) {
        return;
    }

    int j = 0;
    while (j < out_size - 1 &&
           ((*brace >= 'a' && *brace <= 'z') || (*brace >= 'A' && *brace <= 'Z') ||
            (*brace >= '0' && *brace <= '9') || *brace == '_')) {
        out[j++] = *brace++;
    }
    out[j] = '\0';
}

/* ── Client call detection ─────────────────────────────────────── */

/*
 * Detect GraphQL client calls in source code.
 * Patterns:
 *   - gql`query OperationName { ... }`  or  gql`mutation OperationName ...`
 *   - useQuery(GET_USER) / useMutation(CREATE_USER)
 *   - apolloClient.query({ query: GET_USER })
 *   - client.execute("""query GetUser ...""")  (Python)
 *   - @Query("fieldName") (Java Spring GraphQL client annotations)
 */
static int scan_client_calls(const cbm_pipeline_ctx_t *ctx,
                             const cbm_gbuf_node_t *node,
                             const char *source,
                             cbm_sl_consumer_t *cons, int max_cons) {
    int count = 0;
    (void)ctx;

    /* Pattern 1: gql` or gql( with query/mutation/subscription + operation name */
    cbm_regex_t gql_tag_re;
    if (cbm_regcomp(&gql_tag_re,
                     "gql[`(][[:space:]]*[\"'`]?[[:space:]]*(query|mutation|subscription)[[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*)",
                     CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    /* Pattern 2: useQuery / useMutation / useSubscription / useLazyQuery */
    cbm_regex_t use_hook_re;
    if (cbm_regcomp(&use_hook_re,
                     "use(Query|Mutation|Subscription|LazyQuery)\\([[:space:]]*([A-Z][A-Z0-9_]*)",
                     CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&gql_tag_re);
        return 0;
    }

    /* Pattern 3: apolloClient.query / .mutate / .subscribe */
    cbm_regex_t apollo_re;
    if (cbm_regcomp(&apollo_re,
                     "[a-zA-Z_]+\\.(query|mutate|subscribe)\\([[:space:]]*\\{[[:space:]]*query:[[:space:]]*([A-Z][A-Z0-9_]*)",
                     CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&gql_tag_re);
        cbm_regfree(&use_hook_re);
        return 0;
    }

    /* Pattern 4: client.execute with triple-quoted or regular string containing operation name */
    cbm_regex_t execute_re;
    if (cbm_regcomp(&execute_re,
                     "\\.(execute|fetch|request)\\([[:space:]]*[\"`]{1,3}[[:space:]]*(query|mutation|subscription)[[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*)",
                     CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&gql_tag_re);
        cbm_regfree(&use_hook_re);
        cbm_regfree(&apollo_re);
        return 0;
    }

    /* Pattern 5: graphql(` query OperationName ... `) — relay-style */
    cbm_regex_t graphql_fn_re;
    if (cbm_regcomp(&graphql_fn_re,
                     "graphql\\([[:space:]]*`[[:space:]]*(query|mutation|subscription)[[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*)",
                     CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&gql_tag_re);
        cbm_regfree(&use_hook_re);
        cbm_regfree(&apollo_re);
        cbm_regfree(&execute_re);
        return 0;
    }

    const char *p;
    cbm_regmatch_t cm[4];

    /* Scan gql tagged template */
    p = source;
    while (count < max_cons && cbm_regexec(&gql_tag_re, p, 3, cm, 0) == 0) {
        cbm_sl_consumer_t *con = &cons[count];

        /* Extract operation name */
        int nlen = cm[2].rm_eo - cm[2].rm_so;
        if (nlen > (int)sizeof(con->identifier) - 1) {
            nlen = (int)sizeof(con->identifier) - 1;
        }
        memcpy(con->identifier, p + cm[2].rm_so, (size_t)nlen);
        con->identifier[nlen] = '\0';

        snprintf(con->handler_qn, sizeof(con->handler_qn), "%s",
                 node->qualified_name);
        con->handler_id = node->id;
        snprintf(con->file_path, sizeof(con->file_path), "%s",
                 node->file_path);

        /* Extract kind for extra */
        char kind[32] = {0};
        int klen = cm[1].rm_eo - cm[1].rm_so;
        if (klen > (int)sizeof(kind) - 1) {
            klen = (int)sizeof(kind) - 1;
        }
        memcpy(kind, p + cm[1].rm_so, (size_t)klen);
        kind[klen] = '\0';
        snprintf(con->extra, sizeof(con->extra), "%s", kind);
        count++;

        p += cm[0].rm_eo;
    }

    /* Scan React hooks: useQuery(OPERATION_NAME) */
    p = source;
    while (count < max_cons && cbm_regexec(&use_hook_re, p, 3, cm, 0) == 0) {
        cbm_sl_consumer_t *con = &cons[count];

        int nlen = cm[2].rm_eo - cm[2].rm_so;
        if (nlen > (int)sizeof(con->identifier) - 1) {
            nlen = (int)sizeof(con->identifier) - 1;
        }
        memcpy(con->identifier, p + cm[2].rm_so, (size_t)nlen);
        con->identifier[nlen] = '\0';

        snprintf(con->handler_qn, sizeof(con->handler_qn), "%s",
                 node->qualified_name);
        con->handler_id = node->id;
        snprintf(con->file_path, sizeof(con->file_path), "%s",
                 node->file_path);

        /* Map hook type to kind */
        char hook_type[32] = {0};
        int hlen = cm[1].rm_eo - cm[1].rm_so;
        if (hlen > (int)sizeof(hook_type) - 1) {
            hlen = (int)sizeof(hook_type) - 1;
        }
        memcpy(hook_type, p + cm[1].rm_so, (size_t)hlen);
        hook_type[hlen] = '\0';

        if (strcmp(hook_type, "Mutation") == 0) {
            snprintf(con->extra, sizeof(con->extra), "mutation");
        } else if (strcmp(hook_type, "Subscription") == 0) {
            snprintf(con->extra, sizeof(con->extra), "subscription");
        } else {
            snprintf(con->extra, sizeof(con->extra), "query");
        }
        count++;

        p += cm[0].rm_eo;
    }

    /* Scan apolloClient.query({ query: NAME }) */
    p = source;
    while (count < max_cons && cbm_regexec(&apollo_re, p, 3, cm, 0) == 0) {
        cbm_sl_consumer_t *con = &cons[count];

        int nlen = cm[2].rm_eo - cm[2].rm_so;
        if (nlen > (int)sizeof(con->identifier) - 1) {
            nlen = (int)sizeof(con->identifier) - 1;
        }
        memcpy(con->identifier, p + cm[2].rm_so, (size_t)nlen);
        con->identifier[nlen] = '\0';

        snprintf(con->handler_qn, sizeof(con->handler_qn), "%s",
                 node->qualified_name);
        con->handler_id = node->id;
        snprintf(con->file_path, sizeof(con->file_path), "%s",
                 node->file_path);

        char method[32] = {0};
        int mlen = cm[1].rm_eo - cm[1].rm_so;
        if (mlen > (int)sizeof(method) - 1) {
            mlen = (int)sizeof(method) - 1;
        }
        memcpy(method, p + cm[1].rm_so, (size_t)mlen);
        method[mlen] = '\0';

        if (strcmp(method, "mutate") == 0) {
            snprintf(con->extra, sizeof(con->extra), "mutation");
        } else if (strcmp(method, "subscribe") == 0) {
            snprintf(con->extra, sizeof(con->extra), "subscription");
        } else {
            snprintf(con->extra, sizeof(con->extra), "query");
        }
        count++;

        p += cm[0].rm_eo;
    }

    /* Scan .execute / .fetch / .request with inline query */
    p = source;
    while (count < max_cons && cbm_regexec(&execute_re, p, 4, cm, 0) == 0) {
        cbm_sl_consumer_t *con = &cons[count];

        /* cm[3] is the operation name */
        int nlen = cm[3].rm_eo - cm[3].rm_so;
        if (nlen > (int)sizeof(con->identifier) - 1) {
            nlen = (int)sizeof(con->identifier) - 1;
        }
        memcpy(con->identifier, p + cm[3].rm_so, (size_t)nlen);
        con->identifier[nlen] = '\0';

        snprintf(con->handler_qn, sizeof(con->handler_qn), "%s",
                 node->qualified_name);
        con->handler_id = node->id;
        snprintf(con->file_path, sizeof(con->file_path), "%s",
                 node->file_path);

        /* cm[2] is query/mutation/subscription */
        char kind[32] = {0};
        int klen = cm[2].rm_eo - cm[2].rm_so;
        if (klen > (int)sizeof(kind) - 1) {
            klen = (int)sizeof(kind) - 1;
        }
        memcpy(kind, p + cm[2].rm_so, (size_t)klen);
        kind[klen] = '\0';
        snprintf(con->extra, sizeof(con->extra), "%s", kind);
        count++;

        p += cm[0].rm_eo;
    }

    /* Scan graphql(` query OperationName ... `) */
    p = source;
    while (count < max_cons && cbm_regexec(&graphql_fn_re, p, 3, cm, 0) == 0) {
        cbm_sl_consumer_t *con = &cons[count];

        int nlen = cm[2].rm_eo - cm[2].rm_so;
        if (nlen > (int)sizeof(con->identifier) - 1) {
            nlen = (int)sizeof(con->identifier) - 1;
        }
        memcpy(con->identifier, p + cm[2].rm_so, (size_t)nlen);
        con->identifier[nlen] = '\0';

        snprintf(con->handler_qn, sizeof(con->handler_qn), "%s",
                 node->qualified_name);
        con->handler_id = node->id;
        snprintf(con->file_path, sizeof(con->file_path), "%s",
                 node->file_path);

        char kind[32] = {0};
        int klen = cm[1].rm_eo - cm[1].rm_so;
        if (klen > (int)sizeof(kind) - 1) {
            klen = (int)sizeof(kind) - 1;
        }
        memcpy(kind, p + cm[1].rm_so, (size_t)klen);
        kind[klen] = '\0';
        snprintf(con->extra, sizeof(con->extra), "%s", kind);
        count++;

        p += cm[0].rm_eo;
    }

    /* ── Secondary pass: extract first field name from gql body ──── */
    /* For each consumer we just found, try to also extract the first
     * queried field name from the operation body. If it differs from
     * the operation name, add a second consumer entry. */
    int original_count = count;
    for (int ci = 0; ci < original_count && count < max_cons; ci++) {
        cbm_sl_consumer_t *con = &cons[ci];

        /* Search for "query/mutation/subscription OperationName" in the source */
        char search_pattern[512];
        snprintf(search_pattern, sizeof(search_pattern),
                 "%s %s", con->extra[0] ? con->extra : "query", con->identifier);

        const char *op_pos = strstr(source, search_pattern);
        if (op_pos) {
            char field_name[256];
            extract_first_field_name(op_pos, field_name, (int)sizeof(field_name));

            /* Only add if field name differs from operation name and is non-empty */
            if (field_name[0] && strcmp(field_name, con->identifier) != 0) {
                /* Copy via temp to avoid restrict-overlap warning (con and field_con
                 * are in the same heap-allocated cons[] array). */
                cbm_sl_consumer_t tmp;
                memcpy(&tmp, con, sizeof(tmp));
                snprintf(tmp.identifier, sizeof(tmp.identifier), "%s", field_name);

                cons[count] = tmp;
                count++;
            }
        }
    }

    cbm_regfree(&gql_tag_re);
    cbm_regfree(&use_hook_re);
    cbm_regfree(&apollo_re);
    cbm_regfree(&execute_re);
    cbm_regfree(&graphql_fn_re);

    return count;
}

/* ── Is this a GraphQL schema file? ────────────────────────────── */

static bool is_graphql_file(const char *path) {
    const char *ext = sl_file_ext(path);
    return (strcmp(ext, ".graphql") == 0 || strcmp(ext, ".gql") == 0);
}

/* ── Is this a code file we should scan? ───────────────────────── */

static bool is_scannable_code_file(const char *path) {
    const char *ext = sl_file_ext(path);
    return (strcmp(ext, ".ts") == 0 || strcmp(ext, ".tsx") == 0 ||
            strcmp(ext, ".js") == 0 || strcmp(ext, ".jsx") == 0 ||
            strcmp(ext, ".py") == 0 ||
            strcmp(ext, ".go") == 0 ||
            strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0 ||
            strcmp(ext, ".rb") == 0 ||
            strcmp(ext, ".php") == 0);
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_graphql(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "graphql");

    if (cbm_pipeline_check_cancel(ctx)) {
        return -1;
    }

    /* Heap-allocate — these are too large for stack or TLS */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.graphql", "error", "alloc_failed");
        return -1;
    }
    int prod_count = 0;
    int cons_count = 0;

    /* Get Function + Method + Module + Class + Variable nodes from graph buffer */
    const cbm_gbuf_node_t **funcs = NULL;
    const cbm_gbuf_node_t **methods = NULL;
    const cbm_gbuf_node_t **modules = NULL;
    const cbm_gbuf_node_t **classes = NULL;
    const cbm_gbuf_node_t **vars = NULL;
    int nfuncs = 0;
    int nmethods = 0;
    int nmodules = 0;
    int nclasses = 0;
    int nvars = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Function", &funcs, &nfuncs);
    cbm_gbuf_find_by_label(ctx->gbuf, "Method", &methods, &nmethods);
    cbm_gbuf_find_by_label(ctx->gbuf, "Module", &modules, &nmodules);
    cbm_gbuf_find_by_label(ctx->gbuf, "Class", &classes, &nclasses);
    cbm_gbuf_find_by_label(ctx->gbuf, "Variable", &vars, &nvars);

    /* Collect all node sets to iterate */
    struct {
        const cbm_gbuf_node_t **nodes;
        int count;
    } node_sets[5] = {
        { funcs, nfuncs },
        { methods, nmethods },
        { modules, nmodules },
        { classes, nclasses },
        { vars, nvars },
    };

    for (int ns = 0; ns < 5; ns++) {
        for (int i = 0; i < node_sets[ns].count; i++) {
            const cbm_gbuf_node_t *node = node_sets[ns].nodes[i];

            if (cbm_pipeline_check_cancel(ctx)) {
                free(producers);
                free(consumers);
                return -1;
            }

            /* Read source for this node */
            char *source = sl_read_node_source(ctx, node);
            if (!source) {
                continue;
            }

            if (is_graphql_file(node->file_path)) {
                /* SDL file: extract field definitions as producers */
                int n = scan_sdl_file(ctx, node, source,
                                      &producers[prod_count],
                                      SL_MAX_PRODUCERS - prod_count);
                prod_count += n;
            }

            if (is_scannable_code_file(node->file_path) ||
                is_graphql_file(node->file_path)) {
                /* Check for resolvers (producers) */
                int n = scan_resolvers(ctx, node, source,
                                       &producers[prod_count],
                                       SL_MAX_PRODUCERS - prod_count);
                prod_count += n;
            }

            if (is_scannable_code_file(node->file_path)) {
                /* Check for client calls (consumers) */
                int n = scan_client_calls(ctx, node, source,
                                          &consumers[cons_count],
                                          SL_MAX_CONSUMERS - cons_count);
                cons_count += n;
            }

            free(source);
        }
    }

    cbm_log_info("servicelink.graphql.discovery",
                 "producers", itoa_gql(prod_count),
                 "consumers", itoa_gql(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "graphql",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "graphql",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    if (prod_count == 0 || cons_count == 0) {
        cbm_log_info("servicelink.done", "protocol", "graphql",
                     "links", "0");
        free(producers);
        free(consumers);
        return 0;
    }

    /* ── Matching phase ────────────────────────────────────────── */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        cbm_sl_consumer_t *con = &consumers[ci];

        double best_conf = 0.0;
        int best_pi = -1;

        /* Normalize consumer name for comparison */
        char con_norm[256];
        normalize_name(con->identifier, con_norm, (int)sizeof(con_norm));

        for (int pi = 0; pi < prod_count; pi++) {
            cbm_sl_producer_t *prod = &producers[pi];
            double conf = 0.0;

            /* Skip self-links (same file, same function) */
            if (con->handler_id == prod->source_id) {
                continue;
            }

            /* Exact name match */
            if (strcmp(con->identifier, prod->identifier) == 0) {
                conf = CONF_EXACT_MATCH;
            }

            /* Normalized match (camelCase <-> snake_case) */
            if (conf < CONF_NORMALIZED_MATCH) {
                char prod_norm[256];
                normalize_name(prod->identifier, prod_norm,
                               (int)sizeof(prod_norm));
                if (strcmp(con_norm, prod_norm) == 0) {
                    conf = CONF_NORMALIZED_MATCH;
                }
            }

            /* Fuzzy match via normalized Levenshtein */
            if (conf < CONF_FUZZY_MATCH) {
                char prod_norm[256];
                normalize_name(prod->identifier, prod_norm,
                               (int)sizeof(prod_norm));
                double sim = cbm_normalized_levenshtein(con_norm, prod_norm);
                if (sim >= FUZZY_THRESHOLD) {
                    conf = CONF_FUZZY_MATCH;
                }
            }

            if (conf > best_conf) {
                best_conf = conf;
                best_pi = pi;
            }

            /* If we have an exact match, no need to keep searching */
            if (conf >= CONF_EXACT_MATCH) {
                break;
            }
        }

        /* Create edge if confidence is above minimum */
        if (best_pi >= 0 && best_conf >= SL_MIN_CONFIDENCE) {
            cbm_sl_producer_t *prod = &producers[best_pi];

            /* Build extra JSON with operation kind */
            char extra_json[256];
            if (con->extra[0]) {
                snprintf(extra_json, sizeof(extra_json),
                         "\"operation_kind\":\"%s\"", con->extra);
            } else {
                extra_json[0] = '\0';
            }

            sl_insert_edge(ctx, con->handler_id, prod->source_id,
                           SL_EDGE_GRAPHQL, con->identifier,
                           best_conf, extra_json);
            link_count++;
        }
    }

    cbm_log_info("servicelink.done", "protocol", "graphql",
                 "links", itoa_gql(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
