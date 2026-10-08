/* Query-local test-selection policy. Project rules retain their declared order.
 */
#include "foundation/arena.h"
#include "mcp/test_impact.h"
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <yyjson/yyjson.h>

enum {
    TP_MAX_PATTERN = 65536,
    TP_MAX_COMPAT_PATHS = 256,
    TP_MAX_COMPAT_PATH = 1024,
    TP_MAX_EXPANSIONS = 4096,
    TP_MAX_EXPANDED_BYTES = 262144,
    TP_MAX_BRACE_DEPTH = 16,
    TP_MAX_MATCH_BYTES = 65536,
    TP_MATCH_MAX_WORK = 16 * 1024 * 1024
};

typedef enum {
    TP_LITERAL,
    TP_QUESTION,
    TP_STAR,
    TP_DOUBLE_STAR,
    TP_DOUBLE_STAR_SLASH,
    TP_CLASS
} tp_token_kind_t;

typedef struct {
    tp_token_kind_t kind;
    unsigned char literal;
    const unsigned char *class_bits;
} tp_token_t;

typedef struct tp_pattern tp_pattern_t;
struct tp_pattern {
    const char *text;
    size_t len;
    tp_pattern_t *next;
    tp_token_t *tokens;
    size_t token_count;
    bool rooted;
    bool dir_only;
    bool disabled;
};

typedef struct {
    tp_pattern_t *head;
    tp_pattern_t *tail;
    bool perf;
} tp_patterns_t;

typedef struct {
    tp_patterns_t paths;
    tp_patterns_t suites;
} tp_rule_matchers_t;

typedef struct {
    tp_patterns_t suites;
    tp_patterns_t excludes;
} tp_lane_matchers_t;

struct cbm_test_policy {
    CBMArena arena;
    cbm_test_rule_t *rules;
    tp_rule_matchers_t *rule_matchers;
    int rule_count;
    cbm_test_lane_t *lanes;
    tp_lane_matchers_t *lane_matchers;
    int lane_count;
    const char *digest;
    size_t expansion_count;
    size_t expanded_bytes;
    const char **compat_paths;
    int compat_count;
};

/* Expansion uses an acyclic graph with shared continuations. Its explicit
 * traversal stack bounds stack use even for thousands of adjacent groups. */
typedef struct tp_expand_node tp_expand_node_t;
struct tp_expand_node {
    const char *text;
    size_t len;
    tp_expand_node_t *next;
    tp_expand_node_t *alternatives;
    tp_expand_node_t *sibling;
};

typedef struct {
    tp_expand_node_t *head;
    tp_expand_node_t *tail;
} tp_chain_t;

typedef struct {
    CBMArena arena;
    const char *text;
    size_t len;
    size_t pos;
    size_t alternative_count;
    bool valid;
} tp_expander_t;

static yyjson_val *tp_field(yyjson_val *object, const char *name, bool *valid) {
    if (!yyjson_is_obj(object)) {
        *valid = false;
        return NULL;
    }
    yyjson_val *found = NULL, *key, *value;
    size_t index, maximum, len = strlen(name);
    yyjson_obj_foreach(object, index, maximum, key, value) {
        if (yyjson_get_len(key) == len && memcmp(yyjson_get_str(key), name, len) == 0) {
            if (found) {
                *valid = false;
                return NULL;
            }
            found = value;
        }
    }
    return found;
}

static const char *tp_string(yyjson_val *value, size_t *len) {
    const char *text = yyjson_get_str(value);
    size_t n = yyjson_get_len(value);
    if (!text || !n || strlen(text) != n)
        return NULL;
    if (len)
        *len = n;
    return text;
}

static bool tp_pattern_valid(const char *text, size_t len) {
    if (!text || !len || len > TP_MAX_PATTERN || text[0] == '!' || text[0] == '#' ||
        memchr(text, '\0', len) || memchr(text, '\n', len) || memchr(text, '\r', len))
        return false;
    if (text[len - 1] == ' ' || text[len - 1] == '\t') {
        size_t escapes = 0;
        for (size_t i = len - 1; i && text[i - 1] == '\\'; i--)
            escapes++;
        if (!(escapes & 1U))
            return false;
    }
    return true;
}

/* Braces and commas inside a well-formed class are not expansion syntax.
 * An unmatched '[' is a literal; it does not hide later brace syntax. */
static size_t tp_class_end(const char *text, size_t len, size_t open) {
    size_t i = open + 1;
    if (i < len && (text[i] == '!' || text[i] == '^'))
        i++;
    if (i < len && text[i] == ']')
        i++;
    for (; i < len; i++) {
        if (text[i] == '\\' && i + 1 < len) {
            i++;
            continue;
        }
        if (text[i] == ']')
            return i + 1;
    }
    return open + 1;
}

static tp_expand_node_t *tp_expand_node(tp_expander_t *exp, const char *text, size_t len) {
    tp_expand_node_t *node = cbm_arena_calloc(&exp->arena, sizeof(*node));
    if (!node) {
        exp->valid = false;
        return NULL;
    }
    node->text = text;
    node->len = len;
    return node;
}

static void tp_chain_append(tp_chain_t *chain, tp_chain_t add) {
    if (!add.head)
        return;
    if (chain->tail)
        chain->tail->next = add.head;
    else
        chain->head = add.head;
    chain->tail = add.tail;
}

static void tp_chain_literal(tp_expander_t *exp, tp_chain_t *chain, const char *text, size_t len) {
    tp_expand_node_t *node = tp_expand_node(exp, text, len);
    tp_chain_t one = {node, node};
    tp_chain_append(chain, one);
}

static tp_chain_t tp_parse_sequence(tp_expander_t *exp, unsigned depth, bool in_group);

static tp_chain_t tp_parse_group(tp_expander_t *exp, unsigned depth) {
    tp_chain_t result = {0};
    size_t open = exp->pos++;
    if (depth >= TP_MAX_BRACE_DEPTH) {
        exp->valid = false;
        return result;
    }
    tp_expand_node_t *first = NULL, *last = NULL;
    tp_expand_node_t *join = tp_expand_node(exp, NULL, 0);
    size_t count = 0;
    while (exp->valid) {
        tp_chain_t branch = tp_parse_sequence(exp, depth + 1, true);
        if (!exp->valid)
            return result;
        if (!branch.head)
            tp_chain_literal(exp, &branch, NULL, 0);
        if (!exp->valid)
            return result;
        branch.tail->next = join;
        if (last)
            last->sibling = branch.head;
        else
            first = branch.head;
        last = branch.head;
        count++;
        if (exp->pos >= exp->len) {
            exp->valid = false;
            return result;
        }
        if (exp->text[exp->pos] == '}') {
            exp->pos++;
            break;
        }
        /* parse_sequence stops at either a comma or a closing brace. */
        exp->pos++;
    }
    if (!exp->valid)
        return result;
    if (count == 1) {
        /* A group with no comma preserves its braces, while nested groups
         * still expand: {a{b,c}} yields {ab} and {ac}. */
        tp_chain_literal(exp, &result, exp->text + open, 1);
        if (!exp->valid)
            return (tp_chain_t){0};
        result.tail->next = first;
        result.tail = join;
        tp_chain_literal(exp, &result, exp->text + exp->pos - 1, 1);
    } else {
        tp_expand_node_t *choice = tp_expand_node(exp, NULL, 0);
        if (!choice)
            return (tp_chain_t){0};
        choice->alternatives = first;
        result.head = choice;
        result.tail = join;
        exp->alternative_count += count;
    }
    return result;
}

static tp_chain_t tp_parse_sequence(tp_expander_t *exp, unsigned depth, bool in_group) {
    tp_chain_t result = {0};
    size_t literal = exp->pos;
    while (exp->pos < exp->len && exp->valid) {
        char ch = exp->text[exp->pos];
        if (ch == '\\' && exp->pos + 1 < exp->len) {
            exp->pos += 2;
            continue;
        }
        if (ch == '[') {
            exp->pos = tp_class_end(exp->text, exp->len, exp->pos);
            continue;
        }
        if (ch == '}' || (in_group && ch == ',')) {
            if (!in_group)
                exp->valid = false;
            break;
        }
        if (ch == '{') {
            if (exp->pos > literal)
                tp_chain_literal(exp, &result, exp->text + literal, exp->pos - literal);
            tp_chain_t group = tp_parse_group(exp, depth);
            tp_chain_append(&result, group);
            literal = exp->pos;
        } else {
            exp->pos++;
        }
    }
    if (exp->valid && exp->pos > literal)
        tp_chain_literal(exp, &result, exp->text + literal, exp->pos - literal);
    return result;
}

static unsigned char tp_class_char(const char *text, size_t end, size_t *pos) {
    if (text[*pos] == '\\' && *pos + 1 < end)
        (*pos)++;
    return (unsigned char)text[(*pos)++];
}

static bool tp_compile_class(cbm_test_policy_t *policy, const char *text, size_t open, size_t end,
                             tp_token_t *token) {
    unsigned char *bits = cbm_arena_calloc(&policy->arena, 32);
    if (!bits)
        return false;
    size_t pos = open + 1, close = end - 1;
    bool negate = text[pos] == '!' || text[pos] == '^';
    if (negate)
        pos++;
    while (pos < close) {
        unsigned char first = tp_class_char(text, close, &pos);
        if (pos + 1 < close && text[pos] == '-') {
            pos++;
            unsigned char last = tp_class_char(text, close, &pos);
            for (unsigned ch = first; ch <= last; ch++)
                bits[ch >> 3] |= (unsigned char)(1U << (ch & 7U));
        } else {
            bits[first >> 3] |= (unsigned char)(1U << (first & 7U));
        }
    }
    if (negate) {
        for (int i = 0; i < 32; i++)
            bits[i] = (unsigned char)~bits[i];
    }
    /* A class, even a negated class or an explicit slash range, cannot
     * consume a path separator. */
    bits[(unsigned)'/' >> 3] &= (unsigned char)~(1U << ((unsigned)'/' & 7U));
    token->kind = TP_CLASS;
    token->class_bits = bits;
    return true;
}

static bool tp_compile_pattern(cbm_test_policy_t *policy, tp_pattern_t *pattern, bool path_mode) {
    const char *text = pattern->text;
    size_t start = 0, end = pattern->len;
    if (path_mode) {
        if (text[0] == '/') {
            pattern->rooted = true;
            start++;
        }
        size_t escapes = 0;
        for (size_t i = end - 1; i && text[i - 1] == '\\'; i--)
            escapes++;
        if (text[end - 1] == '/' && !(escapes & 1U)) {
            pattern->dir_only = true;
            end--;
        }
        if (end <= start) {
            pattern->disabled = true;
            return true;
        }
        /* As in gitignore discovery, any interior slash anchors a path
         * pattern, even when it appears within a class or is escaped. */
        if (memchr(text + start, '/', end - start))
            pattern->rooted = true;
    }
    pattern->tokens = cbm_arena_calloc(&policy->arena, (end - start) * sizeof(tp_token_t));
    if (!pattern->tokens)
        return false;
    for (size_t pos = start; pos < end;) {
        tp_token_t *token = &pattern->tokens[pattern->token_count++];
        unsigned char ch = (unsigned char)text[pos++];
        if (ch == '\\' && pos < end) {
            token->kind = TP_LITERAL;
            token->literal = (unsigned char)text[pos++];
        } else if (ch == '*') {
            size_t star_start = pos - 1;
            while (pos < end && text[pos] == '*')
                pos++;
            /* Globstar is special only as an entire path component. An
             * embedded run such as a**b is an ordinary star and
             * must neither cross nor consume a separator. */
            bool globstar = pos - star_start >= 2 &&
                            (star_start == start || text[star_start - 1] == '/') &&
                            (pos == end || text[pos] == '/');
            if (globstar && pos < end) {
                token->kind = TP_DOUBLE_STAR_SLASH;
                pattern->rooted = path_mode;
                pos++;
            } else {
                token->kind = globstar ? TP_DOUBLE_STAR : TP_STAR;
            }
        } else if (ch == '?') {
            token->kind = TP_QUESTION;
        } else if (ch == '[') {
            size_t class_end = tp_class_end(text, end, pos - 1);
            if (class_end > pos) {
                if (!tp_compile_class(policy, text, pos - 1, class_end, token))
                    return false;
                pos = class_end;
            } else {
                token->kind = TP_LITERAL;
                token->literal = ch;
            }
        } else {
            token->kind = TP_LITERAL;
            token->literal = ch;
        }
        if (path_mode && token->kind == TP_LITERAL && token->literal == '/')
            pattern->rooted = true;
    }
    return true;
}

static bool tp_add_expansion(cbm_test_policy_t *policy, tp_patterns_t *patterns, const char *text,
                             size_t len, bool semantic_perf) {
    if (!tp_pattern_valid(text, len) || policy->expansion_count >= TP_MAX_EXPANSIONS ||
        len > TP_MAX_EXPANDED_BYTES - policy->expanded_bytes)
        return false;
    policy->expansion_count++;
    policy->expanded_bytes += len;
    if (semantic_perf && len == 5 && memcmp(text, "@perf", 5) == 0) {
        patterns->perf = true;
        return true;
    }
    tp_pattern_t *pattern = cbm_arena_calloc(&policy->arena, sizeof(*pattern));
    if (!pattern)
        return false;
    pattern->text = cbm_arena_strndup(&policy->arena, text, len);
    if (!pattern->text)
        return false;
    pattern->len = len;
    if (!tp_compile_pattern(policy, pattern, !semantic_perf))
        return false;
    if (patterns->tail)
        patterns->tail->next = pattern;
    else
        patterns->head = pattern;
    patterns->tail = pattern;
    return true;
}

typedef struct {
    tp_expand_node_t *node;
    size_t used;
} tp_expand_frame_t;

static bool tp_expand_pattern(cbm_test_policy_t *policy, tp_patterns_t *patterns, const char *text,
                              size_t len, bool semantic_perf) {
    if (!tp_pattern_valid(text, len))
        return false;
    tp_expander_t exp = {.text = text, .len = len, .valid = true};
    cbm_arena_init_lazy(&exp.arena, 4096);
    tp_chain_t chain = tp_parse_sequence(&exp, 0, false);
    if (!exp.valid || exp.pos != len)
        goto invalid;
    size_t capacity = exp.alternative_count + 1;
    tp_expand_frame_t *stack = cbm_arena_alloc(&exp.arena, capacity * sizeof(*stack));
    char *output = cbm_arena_alloc(&exp.arena, TP_MAX_PATTERN + 1U);
    if (!stack || !output)
        goto invalid;
    size_t pending = 0;
    stack[pending++] = (tp_expand_frame_t){chain.head, 0};
    while (pending) {
        tp_expand_frame_t frame = stack[--pending];
        size_t used = frame.used;
        tp_expand_node_t *node = frame.node;
        bool branched = false;
        while (node) {
            if (node->alternatives) {
                size_t first_pending = pending;
                for (tp_expand_node_t *alt = node->alternatives; alt; alt = alt->sibling) {
                    if (pending >= capacity)
                        goto invalid;
                    stack[pending++] = (tp_expand_frame_t){alt, used};
                }
                /* The LIFO stack visits alternatives in their source order. */
                for (size_t left = first_pending, right = pending - 1; left < right;
                     left++, right--) {
                    tp_expand_frame_t swap = stack[left];
                    stack[left] = stack[right];
                    stack[right] = swap;
                }
                branched = true;
                break;
            }
            if (node->len > TP_MAX_PATTERN - used)
                goto invalid;
            if (node->len)
                memcpy(output + used, node->text, node->len);
            used += node->len;
            node = node->next;
        }
        if (!branched) {
            output[used] = '\0';
            if (!tp_add_expansion(policy, patterns, output, used, semantic_perf))
                goto invalid;
        }
    }
    cbm_arena_destroy(&exp.arena);
    return true;
invalid:
    cbm_arena_destroy(&exp.arena);
    return false;
}

/* Public arrays retain original spellings. Expanded patterns are separate,
 * so receipt/reporting clients do not lose the project configuration. */
static bool tp_string_array(cbm_test_policy_t *policy, yyjson_val *value, bool nonempty,
                            const char *const **result, int *count, tp_patterns_t *patterns,
                            bool semantic_perf) {
    if (!yyjson_is_arr(value))
        return false;
    size_t size = yyjson_arr_size(value);
    if ((nonempty && !size) || size > INT_MAX || size > SIZE_MAX / sizeof(char *))
        return false;
    const char **items = size ? cbm_arena_alloc(&policy->arena, size * sizeof(*items)) : NULL;
    if (size && !items)
        return false;
    size_t index, maximum;
    yyjson_val *item;
    yyjson_arr_foreach(value, index, maximum, item) {
        size_t len = 0;
        const char *text = tp_string(item, &len);
        if (!text)
            return false;
        items[index] = cbm_arena_strndup(&policy->arena, text, len);
        if (!items[index] ||
            (patterns && !tp_expand_pattern(policy, patterns, text, len, semantic_perf)))
            return false;
    }
    *result = items;
    *count = (int)size;
    return true;
}

static bool tp_parse_lanes(cbm_test_policy_t *policy, yyjson_val *value) {
    if (!yyjson_is_arr(value) || !yyjson_arr_size(value) || yyjson_arr_size(value) > INT_MAX)
        return false;
    size_t count = yyjson_arr_size(value);
    if (count > SIZE_MAX / sizeof(*policy->lanes) ||
        count > SIZE_MAX / sizeof(*policy->lane_matchers))
        return false;
    policy->lanes = cbm_arena_calloc(&policy->arena, count * sizeof(*policy->lanes));
    policy->lane_matchers =
        cbm_arena_calloc(&policy->arena, count * sizeof(*policy->lane_matchers));
    if (!policy->lanes || !policy->lane_matchers)
        return false;
    policy->lane_count = (int)count;
    size_t index, maximum;
    yyjson_val *value_lane;
    yyjson_arr_foreach(value, index, maximum, value_lane) {
        bool valid = true;
        yyjson_val *name = tp_field(value_lane, "name", &valid);
        yyjson_val *suites = tp_field(value_lane, "suites", &valid);
        yyjson_val *excludes = tp_field(value_lane, "exclude_suites", &valid);
        yyjson_val *signals = tp_field(value_lane, "signals", &valid);
        yyjson_val *default_action = tp_field(value_lane, "default", &valid);
        yyjson_val *narrow = tp_field(value_lane, "narrow", &valid);
        const char *name_text = tp_string(name, NULL);
        const char *default_text = tp_string(default_action, NULL);
        if (!valid || !name_text || !default_text ||
            (strcmp(default_text, "run") != 0 && strcmp(default_text, "skip") != 0) ||
            (narrow && !yyjson_is_bool(narrow)))
            return false;
        for (size_t earlier = 0; earlier < index; earlier++) {
            if (strcmp(policy->lanes[earlier].name, name_text) == 0)
                return false;
        }
        cbm_test_lane_t *lane = &policy->lanes[index];
        tp_lane_matchers_t *matchers = &policy->lane_matchers[index];
        lane->name = cbm_arena_strdup(&policy->arena, name_text);
        lane->default_run = strcmp(default_text, "run") == 0;
        lane->narrow = narrow && yyjson_get_bool(narrow);
        if (!lane->name ||
            !tp_string_array(policy, suites, true, &lane->suites, &lane->suite_count,
                             &matchers->suites, true) ||
            (excludes && !tp_string_array(policy, excludes, false, &lane->exclude_suites,
                                          &lane->exclude_count, &matchers->excludes, true)) ||
            (signals && !tp_string_array(policy, signals, false, &lane->signals,
                                         &lane->signal_count, NULL, false)))
            return false;
    }
    return true;
}

static bool tp_key_segment(yyjson_val *value, int *segment) {
    const char *text = tp_string(value, NULL);
    static const char prefix[] = "path_segment:";
    if (!text || strncmp(text, prefix, sizeof(prefix) - 1) != 0)
        return false;
    text += sizeof(prefix) - 1;
    if (!*text)
        return false;
    unsigned result = 0;
    for (; *text; text++) {
        if (*text < '0' || *text > '9')
            return false;
        unsigned digit = (unsigned)(*text - '0');
        if (result > ((unsigned)INT_MAX - digit) / 10U)
            return false;
        result = result * 10U + digit;
    }
    *segment = (int)result;
    return true;
}

static const char *const tp_build_paths[] = {
    "Makefile*", "CMakeLists.txt", "*.cmake",        "meson.build", "BUILD*",
    "WORKSPACE", "package*.json",  "go.mod",         "go.sum",      "Cargo.*",
    "pom.xml",   "build.gradle*",  "pyproject.toml", "setup.*",     "requirements*.txt"};
static const char *const tp_ci_paths[] = {".github/**",   ".gitlab-ci.yml", ".circleci/**",
                                          "Jenkinsfile*", ".buildkite/**",  "azure-pipelines*.yml"};
static const char *const tp_vendor_paths[] = {"vendor*/**", "third_party/**"};
static const char *const tp_conftest_paths[] = {"conftest.py"};
/* The selection's own configuration: a change to it may loosen what it
 * selects, so it runs everything (smart-ci-design review M-4). */
static const char *const tp_config_paths[] = {".codebase-memory.json"};
static const char *const tp_documentation_paths[] = {"docs/**", "**/*.md", "LICENSE*"};

typedef struct {
    const char *id;
    cbm_test_rule_action_t action;
    const char *const *paths;
    size_t path_count;
} tp_builtin_t;

#define TP_COUNT(array) (sizeof(array) / sizeof((array)[0]))
static const tp_builtin_t tp_builtins[] = {
    {"builtin:test-impact-config", CBM_TEST_RULE_RUN_ALL, tp_config_paths,
     TP_COUNT(tp_config_paths)},
    {"builtin:build", CBM_TEST_RULE_RUN_ALL, tp_build_paths, TP_COUNT(tp_build_paths)},
    {"builtin:ci", CBM_TEST_RULE_RUN_ALL, tp_ci_paths, TP_COUNT(tp_ci_paths)},
    {"builtin:vendor", CBM_TEST_RULE_RUN_ALL, tp_vendor_paths, TP_COUNT(tp_vendor_paths)},
    {"builtin:conftest", CBM_TEST_RULE_RUN_ALL, tp_conftest_paths, TP_COUNT(tp_conftest_paths)},
    {"builtin:documentation", CBM_TEST_RULE_IGNORE, tp_documentation_paths,
     TP_COUNT(tp_documentation_paths)}};

static bool tp_parse_rule(cbm_test_policy_t *policy, yyjson_val *value, int index) {
    bool valid = true;
    yyjson_val *id = tp_field(value, "id", &valid);
    yyjson_val *paths = tp_field(value, "paths", &valid);
    yyjson_val *action = tp_field(value, "action", &valid);
    yyjson_val *lanes = tp_field(value, "lanes", &valid);
    yyjson_val *suites = tp_field(value, "suites", &valid);
    yyjson_val *key = tp_field(value, "key", &valid);
    const char *id_text = tp_string(id, NULL), *action_text = tp_string(action, NULL);
    if (!valid || !id_text || !action_text)
        return false;
    for (int earlier = 0; earlier < index; earlier++) {
        if (strcmp(policy->rules[earlier].id, id_text) == 0)
            return false;
    }
    for (size_t builtin = 0; builtin < TP_COUNT(tp_builtins); builtin++) {
        if (strcmp(tp_builtins[builtin].id, id_text) == 0)
            return false;
    }
    cbm_test_rule_t *rule = &policy->rules[index];
    tp_rule_matchers_t *matchers = &policy->rule_matchers[index];
    rule->key_segment = -1;
    rule->id = cbm_arena_strdup(&policy->arena, id_text);
    if (!rule->id || !tp_string_array(policy, paths, true, &rule->paths, &rule->path_count,
                                      &matchers->paths, false))
        return false;
    if (strcmp(action_text, "run_all") == 0) {
        rule->action = CBM_TEST_RULE_RUN_ALL;
        return !lanes && !suites && !key;
    }
    if (strcmp(action_text, "ignore") == 0) {
        rule->action = CBM_TEST_RULE_IGNORE;
        return !lanes && !suites && !key;
    }
    if (strcmp(action_text, "lanes") == 0) {
        rule->action = CBM_TEST_RULE_LANES;
        return !suites && !key &&
               tp_string_array(policy, lanes, true, &rule->lanes, &rule->lane_count, NULL, false);
    }
    if (strcmp(action_text, "suites") == 0) {
        rule->action = CBM_TEST_RULE_SUITES;
        return !lanes && !key &&
               tp_string_array(policy, suites, true, &rule->suites, &rule->suite_count,
                               &matchers->suites, true);
    }
    if (strcmp(action_text, "referencing_tests") == 0) {
        rule->action = CBM_TEST_RULE_REFERENCING_TESTS;
        return !lanes && !suites && tp_key_segment(key, &rule->key_segment);
    }
    return false;
}

static bool tp_copy_patterns(cbm_test_policy_t *policy, const char *const *source, size_t count,
                             const char *const **result, tp_patterns_t *patterns,
                             bool semantic_perf) {
    const char **items = cbm_arena_alloc(&policy->arena, count * sizeof(*items));
    if (!items)
        return false;
    for (size_t i = 0; i < count; i++) {
        items[i] = cbm_arena_strdup(&policy->arena, source[i]);
        if (!items[i] ||
            !tp_expand_pattern(policy, patterns, source[i], strlen(source[i]), semantic_perf))
            return false;
    }
    *result = items;
    return true;
}

static bool tp_default_lane(cbm_test_policy_t *policy) {
    static const char *const suites[] = {"*"};
    static const char *const excludes[] = {"@perf"};
    policy->lanes = cbm_arena_calloc(&policy->arena, sizeof(*policy->lanes));
    policy->lane_matchers = cbm_arena_calloc(&policy->arena, sizeof(*policy->lane_matchers));
    if (!policy->lanes || !policy->lane_matchers)
        return false;
    policy->lane_count = 1;
    cbm_test_lane_t *lane = policy->lanes;
    lane->name = cbm_arena_strdup(&policy->arena, "unit");
    lane->suite_count = 1;
    lane->exclude_count = 1;
    lane->default_run = true;
    lane->narrow = true;
    return lane->name &&
           tp_copy_patterns(policy, suites, 1, &lane->suites, &policy->lane_matchers->suites,
                            true) &&
           tp_copy_patterns(policy, excludes, 1, &lane->exclude_suites,
                            &policy->lane_matchers->excludes, true);
}

static bool tp_add_builtins(cbm_test_policy_t *policy, int first) {
    for (size_t i = 0; i < TP_COUNT(tp_builtins); i++) {
        const tp_builtin_t *source = &tp_builtins[i];
        cbm_test_rule_t *rule = &policy->rules[first + (int)i];
        rule->id = cbm_arena_strdup(&policy->arena, source->id);
        rule->action = source->action;
        rule->builtin = true;
        rule->key_segment = -1;
        rule->path_count = (int)source->path_count;
        if (!rule->id || !tp_copy_patterns(policy, source->paths, source->path_count, &rule->paths,
                                           &policy->rule_matchers[first + (int)i].paths, false))
            return false;
    }
    return true;
}

/* A repository-relative file path: no root, no backslash, no empty, `.` or
 * `..` segment, no trailing slash. */
static bool tp_relative_path(const char *text, size_t len) {
    if (!text || !len || len > TP_MAX_COMPAT_PATH || text[0] == '/' || text[len - 1] == '/' ||
        memchr(text, '\\', len))
        return false;
    const char *segment = text;
    const char *end = text + len;
    while (segment < end) {
        const char *slash = memchr(segment, '/', (size_t)(end - segment));
        size_t n = (size_t)((slash ? slash : end) - segment);
        if (!n || (n == 1 && segment[0] == '.') ||
            (n == 2 && segment[0] == '.' && segment[1] == '.'))
            return false;
        segment += n + 1;
    }
    return true;
}

/* `coverage`: what a recorded coverage map depends on beyond the code it
 * measures. Strict on keys: an ignored misspelling would make admitting a map
 * easier, never harder. */
static bool tp_parse_coverage(cbm_test_policy_t *policy, yyjson_val *coverage) {
    bool valid = true;
    yyjson_val *paths = tp_field(coverage, "compatibility_paths", &valid);
    if (!valid || yyjson_obj_size(coverage) != (paths ? 1U : 0U) ||
        (paths && !yyjson_is_arr(paths)))
        return false;
    size_t count = paths ? yyjson_arr_size(paths) : 0;
    if (count > TP_MAX_COMPAT_PATHS)
        return false;
    policy->compat_paths =
        cbm_arena_calloc(&policy->arena, (count ? count : 1) * sizeof(*policy->compat_paths));
    if (!policy->compat_paths)
        return false;
    size_t index, maximum;
    yyjson_val *path;
    yyjson_arr_foreach(paths, index, maximum, path) {
        size_t len = 0;
        const char *text = tp_string(path, &len);
        if (!tp_relative_path(text, len))
            return false;
        policy->compat_paths[index] = cbm_arena_strdup(&policy->arena, text);
        if (!policy->compat_paths[index])
            return false;
    }
    policy->compat_count = (int)count;
    return true;
}

static bool tp_parse_policy(cbm_test_policy_t *policy, yyjson_val *impact) {
    bool valid = true;
    yyjson_val *rules = NULL, *lanes = NULL;
    if (impact) {
        yyjson_val *version = tp_field(impact, "version", &valid);
        rules = tp_field(impact, "rules", &valid);
        lanes = tp_field(impact, "lanes", &valid);
        yyjson_val *unmapped = tp_field(impact, "unmapped_files", &valid);
        yyjson_val *coverage = tp_field(impact, "coverage", &valid);
        if (!valid || !yyjson_is_uint(version) || yyjson_get_uint(version) != 1)
            return false;
        if (coverage && !tp_parse_coverage(policy, coverage))
            return false;
        if (unmapped) {
            const char *text = tp_string(unmapped, NULL);
            if (!text || strcmp(text, "run_all") != 0)
                return false;
        }
    }
    if (lanes ? !tp_parse_lanes(policy, lanes) : !tp_default_lane(policy))
        return false;
    size_t project_count = rules ? yyjson_arr_size(rules) : 0;
    if ((rules && !yyjson_is_arr(rules)) || project_count > INT_MAX - TP_COUNT(tp_builtins))
        return false;
    size_t count = project_count + TP_COUNT(tp_builtins);
    if (count > SIZE_MAX / sizeof(*policy->rules) ||
        count > SIZE_MAX / sizeof(*policy->rule_matchers))
        return false;
    policy->rules = cbm_arena_calloc(&policy->arena, count * sizeof(*policy->rules));
    policy->rule_matchers =
        cbm_arena_calloc(&policy->arena, count * sizeof(*policy->rule_matchers));
    if (!policy->rules || !policy->rule_matchers)
        return false;
    policy->rule_count = (int)count;
    size_t index, maximum;
    yyjson_val *rule_value;
    yyjson_arr_foreach(rules, index, maximum, rule_value) {
        if (!tp_parse_rule(policy, rule_value, (int)index))
            return false;
    }
    if (!tp_add_builtins(policy, (int)project_count))
        return false;
    for (size_t i = 0; i < project_count; i++) {
        const cbm_test_rule_t *rule = &policy->rules[i];
        for (int reference = 0; reference < rule->lane_count; reference++) {
            bool found = false;
            for (int lane = 0; lane < policy->lane_count; lane++) {
                if (strcmp(rule->lanes[reference], policy->lanes[lane].name) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found)
                return false;
        }
    }
    return true;
}

cbm_test_policy_t *cbm_test_policy_new(const cbm_test_config_t *config) {
    if (!config)
        return NULL;
    size_t len = 0;
    bool absent = false;
    const char *source = cbm_test_config_source(config, &len, &absent);
    if ((!absent && (!source || !len)) || len > TP_MAX_PATTERN ||
        (len && (!source || memchr(source, '\0', len))))
        return NULL;
    CBMArena arena;
    cbm_arena_init_lazy(&arena, 4096);
    cbm_test_policy_t *policy = cbm_arena_calloc(&arena, sizeof(*policy));
    if (!policy) {
        cbm_arena_destroy(&arena);
        return NULL;
    }
    policy->arena = arena;
    policy->digest = cbm_arena_strdup(&policy->arena, cbm_test_config_digest(config));
    if (!policy->digest)
        goto invalid;
    yyjson_doc *doc = absent ? NULL : yyjson_read(source, len, 0);
    if (!absent && !doc)
        goto invalid;
    bool valid = true;
    yyjson_val *impact = doc ? tp_field(yyjson_doc_get_root(doc), "test_impact", &valid) : NULL;
    valid = valid && tp_parse_policy(policy, impact);
    yyjson_doc_free(doc);
    if (valid)
        return policy;
invalid:
    cbm_test_policy_free(policy);
    return NULL;
}

void cbm_test_policy_free(cbm_test_policy_t *policy) {
    if (!policy)
        return;
    CBMArena arena = policy->arena;
    cbm_arena_destroy(&arena);
}

const cbm_test_rule_t *cbm_test_policy_rules(const cbm_test_policy_t *policy, int *count) {
    if (count)
        *count = policy ? policy->rule_count : 0;
    return policy ? policy->rules : NULL;
}

const cbm_test_lane_t *cbm_test_policy_lanes(const cbm_test_policy_t *policy, int *count) {
    if (count)
        *count = policy ? policy->lane_count : 0;
    return policy ? policy->lanes : NULL;
}

const char *cbm_test_policy_digest(const cbm_test_policy_t *policy) {
    return policy ? policy->digest : "";
}

const char *const *cbm_test_policy_compatibility_paths(const cbm_test_policy_t *policy,
                                                       int *count) {
    if (count)
        *count = policy ? policy->compat_count : 0;
    return policy && policy->compat_count ? (const char *const *)policy->compat_paths : NULL;
}

typedef struct {
    CBMArena arena;
    unsigned char *first;
    unsigned char *second;
    size_t budget;
} tp_match_context_t;

static bool tp_match_length(const char *text, size_t *len) {
    if (!text || !*text)
        return false;
    for (size_t i = 0; i <= TP_MAX_MATCH_BYTES; i++) {
        if (!text[i]) {
            *len = i;
            return true;
        }
    }
    return false;
}

static bool tp_match_begin(tp_match_context_t *context, size_t len) {
    memset(context, 0, sizeof(*context));
    cbm_arena_init_lazy(&context->arena, 4096);
    context->budget = TP_MATCH_MAX_WORK;
    context->first = cbm_arena_alloc(&context->arena, len + 1);
    context->second = cbm_arena_alloc(&context->arena, len + 1);
    if (!context->first || !context->second) {
        cbm_arena_destroy(&context->arena);
        return false;
    }
    return true;
}

/* Unlike the discovery matcher's best-effort API, this evaluator must never
 * turn resource exhaustion into permission to omit a test. Two suffix rows
 * evaluate each (token, input byte) once, with an explicit per-operation
 * budget shared by every rule and directory ancestor. Suffix/ancestor scans
 * also consume one unit per byte. It uses no recursion.
 * False is an error; *matched is only meaningful when true is returned. */
static bool tp_glob_matches(tp_match_context_t *context, const tp_pattern_t *pattern,
                            const char *text, size_t len, bool direct, bool *matched) {
    *matched = false;
    if (pattern->disabled)
        return true;
    unsigned char *next = context->first, *current = context->second;
    if (context->budget < len + 1)
        return false;
    context->budget -= len + 1;
    memset(next, 0, len + 1);
    next[len] = 1;
    for (size_t token_index = pattern->token_count; token_index > 0; token_index--) {
        const tp_token_t *token = &pattern->tokens[token_index - 1];
        if (context->budget < len + 1)
            return false;
        context->budget -= len + 1;
        bool slash_suffix = false;
        for (size_t remaining = len + 1; remaining > 0; remaining--) {
            size_t pos = remaining - 1;
            bool has_char = pos < len;
            unsigned char ch = has_char ? (unsigned char)text[pos] : 0;
            switch (token->kind) {
            case TP_LITERAL:
                current[pos] = has_char && ch == token->literal && next[pos + 1];
                break;
            case TP_QUESTION:
                current[pos] = has_char && ch != '/' && next[pos + 1];
                break;
            case TP_STAR:
                current[pos] = next[pos] || (has_char && ch != '/' && current[pos + 1]);
                break;
            case TP_DOUBLE_STAR:
                current[pos] = next[pos] || (has_char && current[pos + 1]);
                break;
            case TP_DOUBLE_STAR_SLASH:
                if (has_char && ch == '/' && next[pos + 1])
                    slash_suffix = true;
                current[pos] = next[pos] || slash_suffix;
                break;
            case TP_CLASS:
                current[pos] =
                    has_char && (token->class_bits[ch >> 3] & (1U << (ch & 7U))) && next[pos + 1];
                break;
            }
        }
        unsigned char *swap = next;
        next = current;
        current = swap;
    }
    if (next[0]) {
        *matched = true;
        return true;
    }
    if (!direct && !pattern->rooted) {
        for (size_t pos = 0; pos < len; pos++) {
            if (!context->budget)
                return false;
            context->budget--;
            if (text[pos] == '/' && next[pos + 1]) {
                *matched = true;
                break;
            }
        }
    }
    return true;
}

static bool tp_path_patterns_match(tp_match_context_t *context, const tp_patterns_t *patterns,
                                   const char *path, size_t len, bool *matched) {
    *matched = false;
    for (const tp_pattern_t *pattern = patterns->head; pattern; pattern = pattern->next) {
        if (!pattern->dir_only) {
            if (!tp_glob_matches(context, pattern, path, len, false, matched))
                return false;
            if (*matched)
                return true;
        }
        /* Discovery evaluates each ancestor as a directory before reaching
         * its files. Prefix lengths give the same semantics without a copy
         * or allocation whose failure might be mistaken for a non-match. */
        for (size_t end = 1; end < len; end++) {
            if (!context->budget)
                return false;
            context->budget--;
            if (path[end] == '/') {
                if (!tp_glob_matches(context, pattern, path, end, false, matched))
                    return false;
                if (*matched)
                    return true;
            }
        }
    }
    return true;
}

static bool tp_suite_patterns_match(tp_match_context_t *context, const tp_patterns_t *patterns,
                                    const char *suite, size_t len, bool perf, bool *matched) {
    *matched = patterns->perf && perf;
    if (*matched)
        return true;
    for (const tp_pattern_t *pattern = patterns->head; pattern; pattern = pattern->next) {
        if (!tp_glob_matches(context, pattern, suite, len, true, matched))
            return false;
        if (*matched)
            return true;
    }
    return true;
}

bool cbm_test_policy_match_path(const cbm_test_policy_t *policy, const char *path,
                                const cbm_test_rule_t **rule) {
    if (rule)
        *rule = NULL;
    size_t len = 0;
    if (!policy || !rule || !tp_match_length(path, &len))
        return false;
    tp_match_context_t context;
    if (!tp_match_begin(&context, len))
        return false;
    bool valid = true;
    for (int i = 0; i < policy->rule_count; i++) {
        bool matched = false;
        if (!tp_path_patterns_match(&context, &policy->rule_matchers[i].paths, path, len,
                                    &matched)) {
            valid = false;
            break;
        }
        if (matched) {
            *rule = &policy->rules[i];
            break;
        }
    }
    cbm_arena_destroy(&context.arena);
    return valid;
}

bool cbm_test_policy_lane_selects_suite(const cbm_test_policy_t *policy, int lane,
                                        const char *suite, bool perf, bool *selected) {
    if (selected)
        *selected = false;
    size_t len = 0;
    if (!policy || !selected || lane < 0 || lane >= policy->lane_count ||
        !tp_match_length(suite, &len))
        return false;
    tp_match_context_t context;
    if (!tp_match_begin(&context, len))
        return false;
    const tp_lane_matchers_t *matchers = &policy->lane_matchers[lane];
    bool included = false, excluded = false;
    bool valid = tp_suite_patterns_match(&context, &matchers->suites, suite, len, perf, &included);
    if (valid && included)
        valid = tp_suite_patterns_match(&context, &matchers->excludes, suite, len, perf, &excluded);
    if (valid)
        *selected = included && !excluded;
    cbm_arena_destroy(&context.arena);
    return valid;
}

bool cbm_test_policy_rule_selects_suite(const cbm_test_policy_t *policy, int rule,
                                        const char *suite, bool perf, bool *selected) {
    if (selected)
        *selected = false;
    size_t len = 0;
    if (!policy || !selected || rule < 0 || rule >= policy->rule_count ||
        policy->rules[rule].action != CBM_TEST_RULE_SUITES || !tp_match_length(suite, &len))
        return false;
    tp_match_context_t context;
    if (!tp_match_begin(&context, len))
        return false;
    bool matched = false;
    bool valid = tp_suite_patterns_match(&context, &policy->rule_matchers[rule].suites, suite, len,
                                         perf, &matched);
    if (valid)
        *selected = matched;
    cbm_arena_destroy(&context.arena);
    return valid;
}
