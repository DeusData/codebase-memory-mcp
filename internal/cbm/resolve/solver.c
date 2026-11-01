#include "solver.h"
#include "stats.h"

#include <stdlib.h>
#include <string.h>

typedef enum {
    SOLVER_LEAF,
    SOLVER_COPY,
    SOLVER_JOIN,
    SOLVER_SELECT,
    SOLVER_TYPE
} solver_operation_t;
typedef struct {
    cbm_resolve_solver_result_t result;
    solver_operation_t operation;
    cbm_resolve_solver_status_t leaf_status;
    cbm_resolve_solver_value_t leaf_value;
    const cbm_resolve_proof_t *audit;
    size_t *dependency_indices;
    size_t remaining;
    size_t reverse_head;
    const char *selector, *prefix, *suffix;
    bool selected, select_type;
    CBMTypeKind wrapper;
    uint32_t version;
} solver_node_t;
typedef struct {
    size_t dependent;
    size_t next;
} solver_reverse_edge_t;

struct cbm_resolve_solver {
    CBMArena *arena;
    cbm_resolve_proof_pool_t *pool;
    cbm_resolve_solver_limits_t limits;
    solver_node_t **nodes;
    size_t count, capacity, edge_count, selections;
    cbm_resolve_solver_status_t failure;
    bool solved;
    uint64_t steps;
};

static void solver_fail(cbm_resolve_solver_t *solver, cbm_resolve_solver_status_t status) {
    if (solver && (solver->failure == CBM_SOLVER_PENDING || status == CBM_SOLVER_FAILED))
        solver->failure = status;
}

cbm_resolve_solver_t *cbm_resolve_solver_new(CBMArena *arena, cbm_resolve_proof_pool_t *pool,
                                             cbm_resolve_solver_limits_t limits) {
    if (!arena || !pool)
        return NULL;
    cbm_resolve_solver_t *solver = cbm_arena_calloc(arena, sizeof(*solver));
    if (!solver)
        return NULL;
    solver->arena = arena;
    solver->pool = pool;
    solver->limits =
        (cbm_resolve_solver_limits_t){.nodes = limits.nodes ? limits.nodes : SIZE_MAX,
                                      .edges = limits.edges ? limits.edges : SIZE_MAX,
                                      .steps = limits.steps ? limits.steps : UINT64_MAX};
    return solver;
}

static int compare_strings(const void *left, const void *right) {
    return strcmp(*(const char *const *)left, *(const char *const *)right);
}

static bool reserve_node(cbm_resolve_solver_t *solver) {
    if (solver->count < solver->capacity)
        return true;
    size_t capacity = solver->capacity ? solver->capacity * 2 : 32;
    if (capacity < solver->capacity || capacity > SIZE_MAX / sizeof(*solver->nodes))
        return false;
    solver_node_t **nodes = cbm_arena_alloc(solver->arena, capacity * sizeof(*nodes));
    if (!nodes)
        return false;
    if (solver->count)
        memcpy(nodes, solver->nodes, solver->count * sizeof(*nodes));
    solver->nodes = nodes;
    solver->capacity = capacity;
    return true;
}

/* Deep copy the bounded shared transfer vocabulary. Unsupported structures
 * remain unknown rather than borrowing a hybrid arena or normalizing away an
 * unknown union arm. The identity preserves each structured wrapper. */
static const CBMType *owned_type(CBMArena *arena, const CBMType *type, unsigned depth,
                                 const char **identity) {
    if (!type || depth >= 64)
        return NULL;
    const CBMType *result = NULL;
    const char *inner = NULL;
    if (type->kind == CBM_TYPE_NAMED || type->kind == CBM_TYPE_BUILTIN) {
        const char *name = type->kind == CBM_TYPE_NAMED ? type->data.named.qualified_name
                                                        : type->data.builtin.name;
        if (!name || !*name)
            return NULL;
        char *copy = cbm_arena_strdup(arena, name);
        if (!copy)
            return NULL;
        result = type->kind == CBM_TYPE_NAMED ? cbm_type_named(arena, copy)
                                              : cbm_type_builtin(arena, copy);
        *identity = cbm_arena_sprintf(arena, "%u:%zu:%s", (unsigned)type->kind, strlen(name), name);
    } else if (type->kind == CBM_TYPE_POINTER || type->kind == CBM_TYPE_SLICE) {
        const CBMType *child =
            type->kind == CBM_TYPE_POINTER ? type->data.pointer.elem : type->data.slice.elem;
        const CBMType *copy = owned_type(arena, child, depth + 1, &inner);
        if (!copy)
            return NULL;
        result = type->kind == CBM_TYPE_POINTER ? cbm_type_pointer(arena, copy)
                                                : cbm_type_slice(arena, copy);
        *identity = cbm_arena_sprintf(arena, "%u:(%s)", (unsigned)type->kind, inner);
    }
    return result && *identity ? result : NULL;
}

const char *cbm_resolve_solver_type_identity(CBMArena *arena, const CBMType *type) {
    const char *identity = NULL;
    return arena && owned_type(arena, type, 0, &identity) ? identity : NULL;
}

static bool add_node(cbm_resolve_solver_t *solver, const char *key, solver_operation_t operation,
                     cbm_resolve_solver_status_t status, cbm_resolve_solver_value_t value,
                     const char *const *dependencies, size_t count,
                     const cbm_resolve_proof_t *audit) {
    if (!solver || solver->solved || !key || !*key || !audit || (count && !dependencies) ||
        (status == CBM_SOLVER_KNOWN && (!value.target || !*value.target))) {
        solver_fail(solver, CBM_SOLVER_FAILED);
        return false;
    }
    if (solver->failure != CBM_SOLVER_PENDING)
        return solver->failure != CBM_SOLVER_FAILED;
    if (solver->count >= solver->limits.nodes || count > solver->limits.edges ||
        count >= SIZE_MAX / sizeof(const char *)) {
        solver_fail(solver, CBM_SOLVER_LIMITED);
        return true;
    }
    solver_node_t *node = cbm_arena_calloc(solver->arena, sizeof(*node));
    const char **keys = count ? cbm_arena_alloc(solver->arena, count * sizeof(*keys)) : NULL;
    if (!node || (count && !keys) || !reserve_node(solver)) {
        solver_fail(solver, CBM_SOLVER_FAILED);
        return false;
    }
    node->result.key = cbm_arena_strdup(solver->arena, key);
    node->leaf_value = value;
    /* Identities are derived from owned structure, never caller-provided text. */
    node->leaf_value.type_identity = NULL;
    if (value.type) {
        node->leaf_value.type =
            owned_type(solver->arena, value.type, 0, &node->leaf_value.type_identity);
        if (!node->leaf_value.type) {
            node->leaf_value = (cbm_resolve_solver_value_t){0};
            status = CBM_SOLVER_UNSUPPORTED;
        }
    }
    if (value.declaration_identity)
        node->leaf_value.declaration_identity =
            cbm_arena_strdup(solver->arena, value.declaration_identity);
    if (value.target)
        node->leaf_value.target = cbm_arena_strdup(solver->arena, value.target);
    node->audit = cbm_resolve_proof_clone(solver->pool, audit);
    if (!node->result.key || (value.target && !node->leaf_value.target) ||
        (value.declaration_identity && !node->leaf_value.declaration_identity) || !node->audit) {
        solver_fail(solver, solver->pool->failure == CBM_RESOLVE_ANALYSIS_LIMITED
                                ? CBM_SOLVER_LIMITED
                                : CBM_SOLVER_FAILED);
        return solver->failure != CBM_SOLVER_FAILED;
    }
    for (size_t i = 0; i < count; i++) {
        if (!dependencies[i] || !*dependencies[i] ||
            !(keys[i] = cbm_arena_strdup(solver->arena, dependencies[i]))) {
            solver_fail(solver, CBM_SOLVER_FAILED);
            return false;
        }
    }
    if (count > 1)
        qsort(keys, count, sizeof(*keys), compare_strings);
    size_t unique = 0;
    for (size_t i = 0; i < count; i++)
        if (!unique || strcmp(keys[i], keys[unique - 1]))
            keys[unique++] = keys[i];
    if (unique > solver->limits.edges - solver->edge_count) {
        solver_fail(solver, CBM_SOLVER_LIMITED);
        return true;
    }
    node->result.dependencies = keys;
    node->result.dependency_count = unique;
    node->operation = operation;
    node->leaf_status = status;
    node->reverse_head = SIZE_MAX;
    solver->edge_count += unique;
    solver->nodes[solver->count++] = node;
    return true;
}

bool cbm_resolve_solver_leaf(cbm_resolve_solver_t *solver, const char *key,
                             cbm_resolve_solver_status_t status, cbm_resolve_solver_value_t value,
                             const cbm_resolve_proof_t *audit) {
    if (status <= CBM_SOLVER_PENDING || status > CBM_SOLVER_FAILED) {
        solver_fail(solver, CBM_SOLVER_FAILED);
        return false;
    }
    return add_node(solver, key, SOLVER_LEAF, status, value, NULL, 0, audit);
}

bool cbm_resolve_solver_copy(cbm_resolve_solver_t *solver, const char *key, const char *dependency,
                             const cbm_resolve_proof_t *audit) {
    return add_node(solver, key, SOLVER_COPY, CBM_SOLVER_PENDING, (cbm_resolve_solver_value_t){0},
                    &dependency, 1, audit);
}

bool cbm_resolve_solver_join(cbm_resolve_solver_t *solver, const char *key,
                             const char *const *dependencies, size_t count,
                             const cbm_resolve_proof_t *audit) {
    if (!count) {
        solver_fail(solver, CBM_SOLVER_FAILED);
        return false;
    }
    return add_node(solver, key, SOLVER_JOIN, CBM_SOLVER_PENDING, (cbm_resolve_solver_value_t){0},
                    dependencies, count, audit);
}

bool cbm_resolve_solver_select(cbm_resolve_solver_t *solver, const char *key, const char *selector,
                               const char *prefix, const char *suffix,
                               const cbm_resolve_proof_t *audit) {
    if (!prefix || !suffix) {
        solver_fail(solver, CBM_SOLVER_FAILED);
        return false;
    }
    if (!add_node(solver, key, SOLVER_SELECT, CBM_SOLVER_PENDING, (cbm_resolve_solver_value_t){0},
                  &selector, 1, audit))
        return false;
    if (solver->failure != CBM_SOLVER_PENDING)
        return solver->failure != CBM_SOLVER_FAILED;
    solver_node_t *node = solver->nodes[solver->count - 1];
    node->selector = node->result.dependencies[0];
    node->prefix = cbm_arena_strdup(solver->arena, prefix);
    node->suffix = cbm_arena_strdup(solver->arena, suffix);
    if (!node->prefix || !node->suffix) {
        solver_fail(solver, CBM_SOLVER_FAILED);
        return false;
    }
    if (solver->selections >= solver->limits.edges - solver->edge_count)
        solver_fail(solver, CBM_SOLVER_LIMITED);
    else
        solver->selections++;
    return true;
}

bool cbm_resolve_solver_select_type(cbm_resolve_solver_t *solver, const char *key,
                                    const char *selector, const char *prefix, const char *suffix,
                                    const cbm_resolve_proof_t *audit) {
    bool ok = cbm_resolve_solver_select(solver, key, selector, prefix, suffix, audit);
    if (ok && solver->failure == CBM_SOLVER_PENDING)
        solver->nodes[solver->count - 1]->select_type = true;
    return ok;
}

bool cbm_resolve_solver_type(cbm_resolve_solver_t *solver, const char *key, const char *dependency,
                             CBMTypeKind wrapper, uint32_t version,
                             const cbm_resolve_proof_t *audit) {
    if (!add_node(solver, key, SOLVER_TYPE, CBM_SOLVER_PENDING, (cbm_resolve_solver_value_t){0},
                  &dependency, 1, audit))
        return false;
    if (solver->failure == CBM_SOLVER_PENDING) {
        solver_node_t *node = solver->nodes[solver->count - 1];
        node->wrapper = wrapper;
        node->version = version;
    }
    return solver->failure != CBM_SOLVER_FAILED;
}

static int compare_nodes(const void *left, const void *right) {
    return strcmp((*(solver_node_t *const *)left)->result.key,
                  (*(solver_node_t *const *)right)->result.key);
}

static size_t find_node(const cbm_resolve_solver_t *solver, const char *key) {
    size_t low = 0, high = solver->count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        int order = strcmp(solver->nodes[middle]->result.key, key);
        if (order < 0)
            low = middle + 1;
        else
            high = middle;
    }
    return low < solver->count && !strcmp(solver->nodes[low]->result.key, key) ? low : SIZE_MAX;
}

static void heap_push(size_t *heap, size_t *count, size_t value) {
    size_t at = (*count)++;
    while (at && heap[(at - 1) / 2] > value) {
        heap[at] = heap[(at - 1) / 2];
        at = (at - 1) / 2;
    }
    heap[at] = value;
}

static size_t heap_pop(size_t *heap, size_t *count) {
    size_t result = heap[0], value = heap[--*count], at = 0;
    while (at < *count / 2) {
        size_t child = at * 2 + 1;
        if (child + 1 < *count && heap[child + 1] < heap[child])
            child++;
        if (heap[child] >= value)
            break;
        heap[at] = heap[child];
        at = child;
    }
    if (*count)
        heap[at] = value;
    return result;
}

static cbm_resolve_solver_status_t analysis_status(cbm_resolve_analysis_t analysis) {
    switch (analysis) {
    case CBM_RESOLVE_ANALYSIS_COMPLETE:
        return CBM_SOLVER_KNOWN;
    case CBM_RESOLVE_ANALYSIS_LIMITED:
        return CBM_SOLVER_LIMITED;
    case CBM_RESOLVE_ANALYSIS_FAILED:
        return CBM_SOLVER_FAILED;
    case CBM_RESOLVE_ANALYSIS_UNSUPPORTED:
        return CBM_SOLVER_UNSUPPORTED;
    default:
        return CBM_SOLVER_UNKNOWN;
    }
}

static cbm_resolve_result_t outcome(const cbm_resolve_solver_result_t *result) {
    cbm_resolve_result_t value = {.reason = "named_dependency_solver"};
    switch (result->status) {
    case CBM_SOLVER_KNOWN:
        value.binding = CBM_RESOLVE_BINDING_UNIQUE;
        value.domain = CBM_RESOLVE_DOMAIN_PROJECT;
        value.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        value.target = result->value.target;
        value.candidate_count = 1;
        break;
    case CBM_SOLVER_AMBIGUOUS:
        value.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        value.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        value.candidate_count = 2;
        break;
    case CBM_SOLVER_UNSUPPORTED:
        value.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
        break;
    case CBM_SOLVER_LIMITED:
        value.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
        break;
    case CBM_SOLVER_FAILED:
        value.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        break;
    case CBM_SOLVER_PENDING:
        value.analysis = CBM_RESOLVE_ANALYSIS_PENDING;
        break;
    case CBM_SOLVER_UNKNOWN:
        value.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN;
        break;
    }
    return value;
}

static bool same_value(cbm_resolve_solver_value_t left, cbm_resolve_solver_value_t right) {
    return left.target && right.target && !strcmp(left.target, right.target) &&
           left.kind == right.kind && left.module == right.module &&
           left.fact_kind == right.fact_kind &&
           !strcmp(left.type_identity ? left.type_identity : "",
                   right.type_identity ? right.type_identity : "") &&
           !strcmp(left.declaration_identity ? left.declaration_identity : "",
                   right.declaration_identity ? right.declaration_identity : "");
}

static const cbm_resolve_proof_t *missing_proof(cbm_resolve_solver_t *solver,
                                                const solver_node_t *node, const char *key) {
    const cbm_resolve_proof_view_t *audit = cbm_resolve_proof_view(node->audit);
    cbm_resolve_proof_key_t lookup = {.lookup_domain = "named_dependency_solver",
                                      .lookup_key = key,
                                      .detail = "missing_registered_fact",
                                      .lookup_closed = true};
    cbm_resolve_result_t missing = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                    .reason = "missing_registered_fact"};
    return cbm_resolve_proof_leaf(solver->pool, CBM_PROOF_ADAPTER_DOMAIN, &audit->source, &lookup,
                                  &missing);
}

static bool settle(cbm_resolve_solver_t *solver, solver_node_t *node, bool cycle) {
    solver->steps++;
    const cbm_resolve_proof_view_t *audit = cbm_resolve_proof_view(node->audit);
    cbm_resolve_solver_result_t *result = &node->result;
    result->predecessors_closed = !cycle;
    if (node->operation == SOLVER_LEAF) {
        result->status = node->leaf_status;
        if (result->status == CBM_SOLVER_KNOWN)
            result->status = analysis_status(audit->outcome.analysis);
        if (result->status == CBM_SOLVER_KNOWN)
            result->value = node->leaf_value;
        result->proof = node->audit;
        return true;
    }
    size_t count = result->dependency_count;
    const cbm_resolve_proof_t **premises =
        cbm_arena_alloc(solver->arena, (count + 1) * sizeof(*premises));
    if (!premises)
        return false;
    size_t premise_count = 1;
    premises[0] = node->audit;
    result->status = cycle ? CBM_SOLVER_UNKNOWN : analysis_status(audit->outcome.analysis);
    bool have_value = false;
    for (size_t i = 0; i < count; i++) {
        solver->steps++;
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_PROPAGATED_FACTS, 1);
        size_t index = node->dependency_indices[i];
        const cbm_resolve_solver_result_t *dependency =
            index == SIZE_MAX ? NULL : &solver->nodes[index]->result;
        if (cycle && dependency && dependency->status == CBM_SOLVER_PENDING)
            continue; /* The full dependency KEY set remains in the open result. */
        cbm_resolve_solver_status_t status = dependency ? dependency->status : CBM_SOLVER_UNKNOWN;
        const cbm_resolve_proof_t *proof =
            dependency ? dependency->proof : missing_proof(solver, node, result->dependencies[i]);
        if (!proof)
            return false;
        premises[premise_count++] = proof;
        if (status != CBM_SOLVER_KNOWN) {
            if (status > result->status)
                result->status = status;
        } else if (node->operation == SOLVER_SELECT &&
                   !strcmp(result->dependencies[i], node->selector)) {
            continue; /* Selector proves the domain, not the selected value. */
        } else if (!have_value) {
            result->value = dependency->value;
            have_value = true;
        } else if (!same_value(result->value, dependency->value) &&
                   result->status < CBM_SOLVER_AMBIGUOUS) {
            result->status = CBM_SOLVER_AMBIGUOUS;
        }
    }
    if (node->operation == SOLVER_TYPE && result->status == CBM_SOLVER_KNOWN && have_value) {
        const CBMType *type = result->value.type;
        if (!type || (node->wrapper != CBM_TYPE_NAMED && node->wrapper != CBM_TYPE_POINTER &&
                      node->wrapper != CBM_TYPE_SLICE)) {
            result->status = CBM_SOLVER_UNSUPPORTED;
        } else {
            if (node->wrapper == CBM_TYPE_POINTER)
                type = cbm_type_pointer(solver->arena, type);
            else if (node->wrapper == CBM_TYPE_SLICE)
                type = cbm_type_slice(solver->arena, type);
            if (node->wrapper != CBM_TYPE_NAMED)
                result->value.type_identity = cbm_arena_sprintf(
                    solver->arena, "%u:(%s)", (unsigned)node->wrapper, result->value.type_identity);
            if (!type || !result->value.type_identity)
                return false;
            result->value.type = type;
            result->value.fact_kind = CBM_SOLVER_FACT_TYPE;
            result->value.binding_version = node->version;
        }
    }
    if (!have_value && result->status == CBM_SOLVER_KNOWN)
        result->status = CBM_SOLVER_UNKNOWN;
    if (result->status != CBM_SOLVER_KNOWN || !have_value)
        result->value = (cbm_resolve_solver_value_t){0};
    cbm_resolve_proof_key_t key = {.lookup_domain = "named_dependency_solver",
                                   .lookup_key = result->key,
                                   .binding_identity = result->key,
                                   .type_identity = result->value.type_identity,
                                   .declaration_identity = result->value.declaration_identity,
                                   .detail = cycle ? "unsettled_cycle" : "closed_dependency_set",
                                   .flags = result->value.kind};
    cbm_resolve_result_t resolved = outcome(result);
    /* An open cycle may have no settled predecessor proof. Its source audit
     * remains a leaf witness, while predecessors_closed=false prevents any
     * consumer from treating the incomplete derivation as a binding proof. */
    cbm_resolve_proof_kind_t kind = cycle                            ? CBM_PROOF_REGISTRY_LOOKUP
                                    : node->operation == SOLVER_COPY ? CBM_PROOF_NAMED_COPY
                                                                     : CBM_PROOF_NAMED_JOIN;
    result->proof = cbm_resolve_proof_derive(solver->pool, kind, &audit->source, &key, &resolved,
                                             premises, premise_count, !cycle);
    return result->proof != NULL;
}

static void fail_results(cbm_resolve_solver_t *solver) {
    for (size_t i = 0; i < solver->count; i++) {
        cbm_resolve_solver_result_t *result = &solver->nodes[i]->result;
        result->status = solver->failure;
        result->value = (cbm_resolve_solver_value_t){0};
        result->proof = NULL;
        result->predecessors_closed = false;
    }
}

bool cbm_resolve_solver_solve(cbm_resolve_solver_t *solver) {
    if (!solver || solver->solved)
        return false;
    solver->solved = true;
    if (solver->selections > solver->limits.edges - solver->edge_count)
        solver_fail(solver, CBM_SOLVER_LIMITED);
    size_t reserved_edges = solver->edge_count + solver->selections;
    if (solver->count > SIZE_MAX / sizeof(size_t) ||
        reserved_edges > SIZE_MAX / sizeof(solver_reverse_edge_t) ||
        reserved_edges > (UINT64_MAX - solver->count) / 4 ||
        solver->count + (uint64_t)solver->edge_count * 3 + (uint64_t)solver->selections * 4 >
            solver->limits.steps)
        solver_fail(solver, CBM_SOLVER_LIMITED);
    if (solver->failure != CBM_SOLVER_PENDING) {
        fail_results(solver);
        return solver->failure != CBM_SOLVER_FAILED;
    }
    if (solver->count > 1)
        qsort(solver->nodes, solver->count, sizeof(*solver->nodes), compare_nodes);
    for (size_t i = 1; i < solver->count; i++)
        if (!strcmp(solver->nodes[i - 1]->result.key, solver->nodes[i]->result.key))
            solver_fail(solver, CBM_SOLVER_FAILED);
    if (solver->failure != CBM_SOLVER_PENDING) {
        fail_results(solver);
        return false;
    }
    size_t *heap =
        solver->count ? cbm_arena_alloc(solver->arena, solver->count * sizeof(*heap)) : NULL;
    solver_reverse_edge_t *reverse =
        reserved_edges ? cbm_arena_alloc(solver->arena, reserved_edges * sizeof(*reverse)) : NULL;
    if ((solver->count && !heap) || (reserved_edges && !reverse)) {
        solver_fail(solver, CBM_SOLVER_FAILED);
        fail_results(solver);
        return false;
    }
    size_t edge = 0, ready = 0;
    for (size_t n = 0; n < solver->count; n++) {
        solver_node_t *node = solver->nodes[n];
        size_t count = node->result.dependency_count;
        node->dependency_indices =
            count ? cbm_arena_alloc(solver->arena, count * sizeof(*node->dependency_indices))
                  : NULL;
        if (count && !node->dependency_indices) {
            solver_fail(solver, CBM_SOLVER_FAILED);
            break;
        }
        for (size_t d = 0; d < count; d++) {
            solver->steps++;
            size_t target = find_node(solver, node->result.dependencies[d]);
            node->dependency_indices[d] = target;
            if (target != SIZE_MAX) {
                reverse[edge] = (solver_reverse_edge_t){n, solver->nodes[target]->reverse_head};
                solver->nodes[target]->reverse_head = edge++;
                node->remaining++;
            }
        }
        if (!node->remaining)
            heap_push(heap, &ready, n);
    }
    while (ready && solver->failure == CBM_SOLVER_PENDING) {
        size_t n = heap_pop(heap, &ready);
        solver_node_t *node = solver->nodes[n];
        if (node->operation == SOLVER_SELECT && !node->selected) {
            node->selected = true;
            size_t selector_index = node->dependency_indices[0];
            const cbm_resolve_solver_result_t *selector =
                selector_index == SIZE_MAX ? NULL : &solver->nodes[selector_index]->result;
            const char *identity = selector ? (node->select_type ? selector->value.type_identity
                                                                 : selector->value.target)
                                            : NULL;
            if (selector && selector->status == CBM_SOLVER_KNOWN && identity && *identity) {
                const char *selected = cbm_arena_sprintf(solver->arena, "%s%s%s", node->prefix,
                                                         identity, node->suffix);
                const char **keys = cbm_arena_alloc(solver->arena, 2 * sizeof(*keys));
                size_t *indices = cbm_arena_alloc(solver->arena, 2 * sizeof(*indices));
                if (!selected || !keys || !indices) {
                    solver_fail(solver, CBM_SOLVER_FAILED);
                    break;
                }
                keys[0] = node->selector;
                keys[1] = selected;
                if (strcmp(keys[0], keys[1]) > 0) {
                    const char *swap = keys[0];
                    keys[0] = keys[1];
                    keys[1] = swap;
                }
                indices[0] = find_node(solver, keys[0]);
                indices[1] = find_node(solver, keys[1]);
                node->result.dependencies = keys;
                node->result.dependency_count = 2;
                node->dependency_indices = indices;
                size_t target = find_node(solver, selected);
                solver->steps++;
                if (target != SIZE_MAX &&
                    solver->nodes[target]->result.status == CBM_SOLVER_PENDING) {
                    reverse[edge] = (solver_reverse_edge_t){n, solver->nodes[target]->reverse_head};
                    solver->nodes[target]->reverse_head = edge++;
                    node->remaining = 1;
                    continue;
                }
            }
        }
        if (!settle(solver, node, false)) {
            solver_fail(solver, solver->pool->failure == CBM_RESOLVE_ANALYSIS_LIMITED
                                    ? CBM_SOLVER_LIMITED
                                    : CBM_SOLVER_FAILED);
            break;
        }
        for (size_t r = node->reverse_head; r != SIZE_MAX; r = reverse[r].next) {
            solver->steps++;
            size_t dependent = reverse[r].dependent;
            if (!--solver->nodes[dependent]->remaining)
                heap_push(heap, &ready, dependent);
        }
    }
    for (size_t n = 0; n < solver->count && solver->failure == CBM_SOLVER_PENDING; n++) {
        if (solver->nodes[n]->result.status == CBM_SOLVER_PENDING &&
            !settle(solver, solver->nodes[n], true))
            solver_fail(solver, solver->pool->failure == CBM_RESOLVE_ANALYSIS_LIMITED
                                    ? CBM_SOLVER_LIMITED
                                    : CBM_SOLVER_FAILED);
    }
    if (solver->failure != CBM_SOLVER_PENDING)
        fail_results(solver);
    return solver->failure != CBM_SOLVER_FAILED;
}

void cbm_resolve_solver_get(const cbm_resolve_solver_t *solver, const char *key,
                            cbm_resolve_solver_result_t *out) {
    if (!out)
        return;
    *out = (cbm_resolve_solver_result_t){.key = key, .status = CBM_SOLVER_PENDING};
    if (!solver || !key) {
        out->status = CBM_SOLVER_FAILED;
        return;
    }
    if (!solver->solved)
        return;
    if (solver->failure != CBM_SOLVER_PENDING) {
        out->status = solver->failure;
        return;
    }
    size_t index = find_node(solver, key);
    if (index != SIZE_MAX)
        *out = solver->nodes[index]->result;
    else
        out->status = CBM_SOLVER_UNKNOWN;
}

size_t cbm_resolve_solver_count(const cbm_resolve_solver_t *solver) {
    return cbm_resolve_solver_complete(solver) ? solver->count : 0;
}

const cbm_resolve_solver_result_t *cbm_resolve_solver_at(const cbm_resolve_solver_t *solver,
                                                         size_t index) {
    return cbm_resolve_solver_complete(solver) && index < solver->count
               ? &solver->nodes[index]->result
               : NULL;
}

uint64_t cbm_resolve_solver_steps(const cbm_resolve_solver_t *solver) {
    return solver ? solver->steps : 0;
}

bool cbm_resolve_solver_complete(const cbm_resolve_solver_t *solver) {
    return solver && solver->solved && solver->failure == CBM_SOLVER_PENDING;
}
