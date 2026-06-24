/*
 * php_stdlib_data.c — corpus-seeded PHP stdlib + framework type data.
 *
 * Strategy from docs/PLAN_PHP_LSP_INTEGRATION.md §6:
 *   1. Genuinely-stdlib base — SPL containers/iterators, PSR interfaces,
 *      DateTime, Throwable hierarchy.
 *   2. Top-N corpus-driven types from laravel/framework benchmark
 *      (Model in=836, Builder, Container, Collection, etc.).
 *
 * Each entry is a one-liner. Sources commented inline.
 *
 * Class QNs use dotted form (Foo.Bar.Baz) to match how the unified
 * extractor + php_resolve_class_name produce names.
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../../arena.h"
#include "../php_lsp.h"
#include <string.h>

#define REG_TYPE(qn_, short_, is_iface_, parents_) do { \
    memset(&rt, 0, sizeof(rt));                          \
    rt.qualified_name = (qn_);                           \
    rt.short_name = (short_);                            \
    rt.is_interface = (is_iface_);                       \
    rt.embedded_types = (parents_);                      \
    cbm_registry_add_type(reg, rt);                      \
} while (0)

#define REG_METHOD(class_qn_, method_name_, ret_type_) do {                        \
    memset(&rf, 0, sizeof(rf));                                                    \
    rf.min_params = -1;                                                            \
    rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", (class_qn_), (method_name_)); \
    rf.short_name = (method_name_);                                                \
    rf.receiver_type = (class_qn_);                                                \
    {                                                                              \
        const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
        rets[0] = (ret_type_); rets[1] = NULL;                                     \
        rf.signature = cbm_type_func(arena, NULL, NULL, rets);                     \
    }                                                                              \
    cbm_registry_add_func(reg, rf);                                                \
} while (0)

#define MIXED cbm_type_unknown()

void cbm_php_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    CBMRegisteredType rt;
    CBMRegisteredFunc rf;

    /* ── Throwable hierarchy ────────────────────────────────────── */
    static const char *throwable_parents[] = {NULL};
    static const char *exception_parents[] = {"Throwable", NULL};
    static const char *error_parents[] = {"Throwable", NULL};
    static const char *runtime_exception_parents[] = {"Exception", NULL};
    static const char *logic_exception_parents[] = {"Exception", NULL};
    static const char *invalid_argument_parents[] = {"LogicException", NULL};
    static const char *type_error_parents[] = {"Error", NULL};
    static const char *value_error_parents[] = {"Error", NULL};

    REG_TYPE("Throwable", "Throwable", true, throwable_parents);
    REG_TYPE("Exception", "Exception", false, exception_parents);
    REG_TYPE("Error", "Error", false, error_parents);
    REG_TYPE("RuntimeException", "RuntimeException", false, runtime_exception_parents);
    REG_TYPE("LogicException", "LogicException", false, logic_exception_parents);
    REG_TYPE("InvalidArgumentException", "InvalidArgumentException", false,
             invalid_argument_parents);
    REG_TYPE("TypeError", "TypeError", false, type_error_parents);
    REG_TYPE("ValueError", "ValueError", false, value_error_parents);

    REG_METHOD("Throwable", "getMessage", cbm_type_builtin(arena, "string"));
    REG_METHOD("Throwable", "getCode", cbm_type_builtin(arena, "int"));
    REG_METHOD("Throwable", "getFile", cbm_type_builtin(arena, "string"));
    REG_METHOD("Throwable", "getLine", cbm_type_builtin(arena, "int"));
    REG_METHOD("Throwable", "getTrace", cbm_type_builtin(arena, "array"));
    REG_METHOD("Throwable", "getTraceAsString", cbm_type_builtin(arena, "string"));
    REG_METHOD("Throwable", "getPrevious", cbm_type_named(arena, "Throwable"));

    /* ── DateTime family ────────────────────────────────────────── */
    static const char *datetime_iface_parents[] = {NULL};
    static const char *datetime_parents[] = {"DateTimeInterface", NULL};
    static const char *datetime_immutable_parents[] = {"DateTimeInterface", NULL};

    REG_TYPE("DateTimeInterface", "DateTimeInterface", true, datetime_iface_parents);
    REG_TYPE("DateTime", "DateTime", false, datetime_parents);
    REG_TYPE("DateTimeImmutable", "DateTimeImmutable", false, datetime_immutable_parents);
    REG_TYPE("DateInterval", "DateInterval", false, throwable_parents);
    REG_TYPE("DateTimeZone", "DateTimeZone", false, throwable_parents);

    REG_METHOD("DateTimeInterface", "format", cbm_type_builtin(arena, "string"));
    REG_METHOD("DateTimeInterface", "getTimestamp", cbm_type_builtin(arena, "int"));
    REG_METHOD("DateTimeInterface", "getOffset", cbm_type_builtin(arena, "int"));
    REG_METHOD("DateTime", "modify", cbm_type_named(arena, "DateTime"));
    REG_METHOD("DateTime", "add", cbm_type_named(arena, "DateTime"));
    REG_METHOD("DateTime", "sub", cbm_type_named(arena, "DateTime"));
    REG_METHOD("DateTime", "setDate", cbm_type_named(arena, "DateTime"));
    REG_METHOD("DateTime", "setTime", cbm_type_named(arena, "DateTime"));
    REG_METHOD("DateTimeImmutable", "modify", cbm_type_named(arena, "DateTimeImmutable"));
    REG_METHOD("DateTimeImmutable", "add", cbm_type_named(arena, "DateTimeImmutable"));
    REG_METHOD("DateTimeImmutable", "sub", cbm_type_named(arena, "DateTimeImmutable"));

    /* ── SPL containers / iterators ─────────────────────────────── */
    static const char *traversable_parents[] = {NULL};
    static const char *iterator_parents[] = {"Traversable", NULL};
    static const char *iterator_aggregate_parents[] = {"Traversable", NULL};
    static const char *generator_parents[] = {"Iterator", NULL};
    static const char *array_iterator_parents[] = {"Iterator", "Countable", "ArrayAccess",
                                                   "Serializable", NULL};
    static const char *array_object_parents[] = {"IteratorAggregate", "Countable",
                                                 "ArrayAccess", "Serializable", NULL};
    static const char *spl_doubly_linked_list_parents[] = {"Iterator", "Countable",
                                                           "ArrayAccess", "Serializable", NULL};
    static const char *spl_stack_parents[] = {"SplDoublyLinkedList", NULL};
    static const char *spl_queue_parents[] = {"SplDoublyLinkedList", NULL};
    static const char *spl_object_storage_parents[] = {"Countable", "Iterator", "Serializable",
                                                       "ArrayAccess", NULL};
    static const char *countable_parents[] = {NULL};
    static const char *array_access_parents[] = {NULL};
    static const char *stringable_parents[] = {NULL};

    REG_TYPE("Traversable", "Traversable", true, traversable_parents);
    REG_TYPE("Iterator", "Iterator", true, iterator_parents);
    REG_TYPE("IteratorAggregate", "IteratorAggregate", true, iterator_aggregate_parents);
    REG_TYPE("Generator", "Generator", false, generator_parents);
    REG_TYPE("ArrayIterator", "ArrayIterator", false, array_iterator_parents);
    REG_TYPE("ArrayObject", "ArrayObject", false, array_object_parents);
    REG_TYPE("SplDoublyLinkedList", "SplDoublyLinkedList", false, spl_doubly_linked_list_parents);
    REG_TYPE("SplStack", "SplStack", false, spl_stack_parents);
    REG_TYPE("SplQueue", "SplQueue", false, spl_queue_parents);
    REG_TYPE("SplObjectStorage", "SplObjectStorage", false, spl_object_storage_parents);
    REG_TYPE("Countable", "Countable", true, countable_parents);
    REG_TYPE("ArrayAccess", "ArrayAccess", true, array_access_parents);
    REG_TYPE("Stringable", "Stringable", true, stringable_parents);
    REG_TYPE("Serializable", "Serializable", true, traversable_parents);

    REG_METHOD("Iterator", "current", MIXED);
    REG_METHOD("Iterator", "key", MIXED);
    REG_METHOD("Iterator", "next", cbm_type_builtin(arena, "void"));
    REG_METHOD("Iterator", "rewind", cbm_type_builtin(arena, "void"));
    REG_METHOD("Iterator", "valid", cbm_type_builtin(arena, "bool"));
    REG_METHOD("Countable", "count", cbm_type_builtin(arena, "int"));
    REG_METHOD("ArrayAccess", "offsetExists", cbm_type_builtin(arena, "bool"));
    REG_METHOD("ArrayAccess", "offsetGet", MIXED);
    REG_METHOD("ArrayAccess", "offsetSet", cbm_type_builtin(arena, "void"));
    REG_METHOD("ArrayAccess", "offsetUnset", cbm_type_builtin(arena, "void"));
    REG_METHOD("Stringable", "__toString", cbm_type_builtin(arena, "string"));
    REG_METHOD("ArrayObject", "getIterator", cbm_type_named(arena, "ArrayIterator"));

    /* ── PSR interfaces (de facto stdlib) ───────────────────────── */
    static const char *psr_logger_parents[] = {NULL};
    static const char *psr_container_parents[] = {NULL};
    static const char *psr_request_parents[] = {NULL};
    static const char *psr_response_parents[] = {NULL};

    REG_TYPE("Psr.Log.LoggerInterface", "LoggerInterface", true, psr_logger_parents);
    REG_TYPE("Psr.Container.ContainerInterface", "ContainerInterface", true,
             psr_container_parents);
    REG_TYPE("Psr.Container.NotFoundExceptionInterface", "NotFoundExceptionInterface", true,
             psr_container_parents);
    REG_TYPE("Psr.Http.Message.RequestInterface", "RequestInterface", true, psr_request_parents);
    REG_TYPE("Psr.Http.Message.ResponseInterface", "ResponseInterface", true,
             psr_response_parents);
    REG_TYPE("Psr.Http.Message.ServerRequestInterface", "ServerRequestInterface", true,
             psr_request_parents);
    REG_TYPE("Psr.Http.Message.UriInterface", "UriInterface", true, psr_request_parents);

    REG_METHOD("Psr.Log.LoggerInterface", "info", cbm_type_builtin(arena, "void"));
    REG_METHOD("Psr.Log.LoggerInterface", "warning", cbm_type_builtin(arena, "void"));
    REG_METHOD("Psr.Log.LoggerInterface", "error", cbm_type_builtin(arena, "void"));
    REG_METHOD("Psr.Log.LoggerInterface", "debug", cbm_type_builtin(arena, "void"));
    REG_METHOD("Psr.Log.LoggerInterface", "critical", cbm_type_builtin(arena, "void"));
    REG_METHOD("Psr.Log.LoggerInterface", "log", cbm_type_builtin(arena, "void"));
    REG_METHOD("Psr.Container.ContainerInterface", "get", MIXED);
    REG_METHOD("Psr.Container.ContainerInterface", "has", cbm_type_builtin(arena, "bool"));
    REG_METHOD("Psr.Http.Message.RequestInterface", "getMethod", cbm_type_builtin(arena, "string"));
    REG_METHOD("Psr.Http.Message.RequestInterface", "getUri",
               cbm_type_named(arena, "Psr.Http.Message.UriInterface"));
    REG_METHOD("Psr.Http.Message.ResponseInterface", "getStatusCode",
               cbm_type_builtin(arena, "int"));
    REG_METHOD("Psr.Http.Message.ResponseInterface", "getBody", MIXED);

    /* ── Closure / fibers ──────────────────────────────────────── */
    REG_TYPE("Closure", "Closure", false, throwable_parents);
    REG_METHOD("Closure", "bindTo", cbm_type_named(arena, "Closure"));
    REG_METHOD("Closure", "call", MIXED);
    REG_METHOD("Closure", "__invoke", MIXED);

    /* ── Laravel framework — top-N corpus-seeded ────────────────── */
    /* These are NOT stdlib; they are seeded because laravel/framework is the
     * canonical PHP benchmark corpus and dominates real-world receiver-type
     * resolution. Comments cite the inbound-edge count from the Q3 row of
     * docs/BENCHMARK.md and from the Q3 search_graph response. */

    static const char *facade_parents[] = {NULL};
    REG_TYPE("Illuminate.Support.Facades.Facade", "Facade", false, facade_parents);
    REG_METHOD("Illuminate.Support.Facades.Facade", "__callStatic", MIXED);
    REG_METHOD("Illuminate.Support.Facades.Facade", "getFacadeRoot", MIXED);

    /* Each named facade extends Facade so __callStatic detection lights up. */
    static const char *facade_child_parents[] = {"Illuminate.Support.Facades.Facade", NULL};
    REG_TYPE("Illuminate.Support.Facades.DB", "DB", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Cache", "Cache", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Auth", "Auth", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Log", "Log", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Route", "Route", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Schema", "Schema", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Storage", "Storage", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Mail", "Mail", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Queue", "Queue", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Event", "Event", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Config", "Config", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.Session", "Session", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.URL", "URL", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.View", "View", false, facade_child_parents);
    REG_TYPE("Illuminate.Support.Facades.App", "App", false, facade_child_parents);

    /* ── Eloquent + Query Builder ───────────────────────────────── *
     *
     * The query builder is the single highest-leverage chain in any Laravel
     * codebase: `Model::query()->where(...)->orderBy(...)->first()`. We
     * register a fluent builder where every "where/order/group/select/take/
     * skip/limit/distinct/join" method returns `Builder` so chains keep
     * resolving. Terminal methods (get/first/find/value/exists) return
     * Collection or the model. */

    REG_TYPE("Illuminate.Database.Eloquent.Builder", "Builder", false, throwable_parents);
    REG_TYPE("Illuminate.Database.Query.Builder", "Builder", false, throwable_parents);
    REG_TYPE("Illuminate.Database.Eloquent.Model", "Model", false, throwable_parents);
    REG_TYPE("Illuminate.Database.Eloquent.Collection", "Collection", false,
             traversable_parents);
    REG_TYPE("Illuminate.Support.Collection", "Collection", false, traversable_parents);

    /* Eloquent Builder chain methods. */
    {
        const char *eb = "Illuminate.Database.Eloquent.Builder";
        const CBMType *self = cbm_type_named(arena, "Illuminate.Database.Eloquent.Builder");
        const CBMType *coll = cbm_type_named(arena, "Illuminate.Database.Eloquent.Collection");
        const CBMType *model = cbm_type_named(arena, "Illuminate.Database.Eloquent.Model");
        REG_METHOD(eb, "where", self);
        REG_METHOD(eb, "orWhere", self);
        REG_METHOD(eb, "whereIn", self);
        REG_METHOD(eb, "whereNull", self);
        REG_METHOD(eb, "whereNotNull", self);
        REG_METHOD(eb, "whereHas", self);
        REG_METHOD(eb, "with", self);
        REG_METHOD(eb, "without", self);
        REG_METHOD(eb, "select", self);
        REG_METHOD(eb, "distinct", self);
        REG_METHOD(eb, "orderBy", self);
        REG_METHOD(eb, "orderByDesc", self);
        REG_METHOD(eb, "groupBy", self);
        REG_METHOD(eb, "having", self);
        REG_METHOD(eb, "join", self);
        REG_METHOD(eb, "leftJoin", self);
        REG_METHOD(eb, "rightJoin", self);
        REG_METHOD(eb, "limit", self);
        REG_METHOD(eb, "take", self);
        REG_METHOD(eb, "skip", self);
        REG_METHOD(eb, "offset", self);
        REG_METHOD(eb, "tap", self);
        REG_METHOD(eb, "when", self);
        REG_METHOD(eb, "unless", self);
        REG_METHOD(eb, "scopeQuery", self);
        REG_METHOD(eb, "get", coll);
        REG_METHOD(eb, "all", coll);
        REG_METHOD(eb, "pluck", coll);
        REG_METHOD(eb, "first", model);
        REG_METHOD(eb, "firstOrFail", model);
        REG_METHOD(eb, "find", model);
        REG_METHOD(eb, "findOrFail", model);
        REG_METHOD(eb, "create", model);
        REG_METHOD(eb, "firstOrCreate", model);
        REG_METHOD(eb, "updateOrCreate", model);
        REG_METHOD(eb, "exists", cbm_type_builtin(arena, "bool"));
        REG_METHOD(eb, "count", cbm_type_builtin(arena, "int"));
        REG_METHOD(eb, "sum", cbm_type_builtin(arena, "int"));
        REG_METHOD(eb, "value", MIXED);
        REG_METHOD(eb, "toSql", cbm_type_builtin(arena, "string"));
    }

    /* Eloquent Model: convenience static-call entry points. */
    {
        const char *em = "Illuminate.Database.Eloquent.Model";
        const CBMType *eb = cbm_type_named(arena, "Illuminate.Database.Eloquent.Builder");
        const CBMType *self = cbm_type_named(arena, "Illuminate.Database.Eloquent.Model");
        const CBMType *coll = cbm_type_named(arena, "Illuminate.Database.Eloquent.Collection");
        REG_METHOD(em, "query", eb);
        REG_METHOD(em, "newQuery", eb);
        REG_METHOD(em, "where", eb);
        REG_METHOD(em, "with", eb);
        REG_METHOD(em, "all", coll);
        REG_METHOD(em, "find", self);
        REG_METHOD(em, "findOrFail", self);
        REG_METHOD(em, "create", self);
        REG_METHOD(em, "save", cbm_type_builtin(arena, "bool"));
        REG_METHOD(em, "update", cbm_type_builtin(arena, "bool"));
        REG_METHOD(em, "delete", cbm_type_builtin(arena, "bool"));
        REG_METHOD(em, "fresh", self);
        REG_METHOD(em, "refresh", self);
        REG_METHOD(em, "load", self);
        REG_METHOD(em, "loadMissing", self);
        REG_METHOD(em, "toArray", cbm_type_builtin(arena, "array"));
        REG_METHOD(em, "toJson", cbm_type_builtin(arena, "string"));
        REG_METHOD(em, "getKey", MIXED);
        REG_METHOD(em, "getAttribute", MIXED);
    }

    /* Illuminate\Support\Collection — chain returns self. */
    {
        const char *coll_qn = "Illuminate.Support.Collection";
        const CBMType *self = cbm_type_named(arena, "Illuminate.Support.Collection");
        REG_METHOD(coll_qn, "map", self);
        REG_METHOD(coll_qn, "filter", self);
        REG_METHOD(coll_qn, "reject", self);
        REG_METHOD(coll_qn, "where", self);
        REG_METHOD(coll_qn, "values", self);
        REG_METHOD(coll_qn, "keys", self);
        REG_METHOD(coll_qn, "sort", self);
        REG_METHOD(coll_qn, "sortBy", self);
        REG_METHOD(coll_qn, "sortByDesc", self);
        REG_METHOD(coll_qn, "groupBy", self);
        REG_METHOD(coll_qn, "keyBy", self);
        REG_METHOD(coll_qn, "merge", self);
        REG_METHOD(coll_qn, "concat", self);
        REG_METHOD(coll_qn, "unique", self);
        REG_METHOD(coll_qn, "take", self);
        REG_METHOD(coll_qn, "skip", self);
        REG_METHOD(coll_qn, "chunk", self);
        REG_METHOD(coll_qn, "flatten", self);
        REG_METHOD(coll_qn, "flatMap", self);
        REG_METHOD(coll_qn, "pluck", self);
        REG_METHOD(coll_qn, "tap", self);
        REG_METHOD(coll_qn, "each", self);
        REG_METHOD(coll_qn, "reverse", self);
        REG_METHOD(coll_qn, "first", MIXED);
        REG_METHOD(coll_qn, "last", MIXED);
        REG_METHOD(coll_qn, "count", cbm_type_builtin(arena, "int"));
        REG_METHOD(coll_qn, "isEmpty", cbm_type_builtin(arena, "bool"));
        REG_METHOD(coll_qn, "isNotEmpty", cbm_type_builtin(arena, "bool"));
        REG_METHOD(coll_qn, "contains", cbm_type_builtin(arena, "bool"));
        REG_METHOD(coll_qn, "toArray", cbm_type_builtin(arena, "array"));
        REG_METHOD(coll_qn, "toJson", cbm_type_builtin(arena, "string"));
        REG_METHOD(coll_qn, "implode", cbm_type_builtin(arena, "string"));
        REG_METHOD(coll_qn, "sum", cbm_type_builtin(arena, "int"));
        REG_METHOD(coll_qn, "avg", cbm_type_builtin(arena, "float"));
        REG_METHOD(coll_qn, "max", MIXED);
        REG_METHOD(coll_qn, "min", MIXED);
    }

    /* ── Symfony HttpFoundation (used by Laravel + Symfony) ─────── */
    static const char *symfony_request_parents[] = {NULL};
    REG_TYPE("Symfony.Component.HttpFoundation.Request", "Request", false,
             symfony_request_parents);
    REG_TYPE("Symfony.Component.HttpFoundation.Response", "Response", false,
             symfony_request_parents);
    REG_TYPE("Symfony.Component.HttpFoundation.HeaderBag", "HeaderBag", false,
             symfony_request_parents);
    REG_TYPE("Symfony.Component.HttpFoundation.ParameterBag", "ParameterBag", false,
             symfony_request_parents);
    REG_METHOD("Symfony.Component.HttpFoundation.Request", "getMethod",
               cbm_type_builtin(arena, "string"));
    REG_METHOD("Symfony.Component.HttpFoundation.Request", "getPathInfo",
               cbm_type_builtin(arena, "string"));
    REG_METHOD("Symfony.Component.HttpFoundation.Request", "getUri",
               cbm_type_builtin(arena, "string"));
    REG_METHOD("Symfony.Component.HttpFoundation.Response", "getStatusCode",
               cbm_type_builtin(arena, "int"));
    REG_METHOD("Symfony.Component.HttpFoundation.Response", "getContent",
               cbm_type_builtin(arena, "string"));

    /* ── Carbon (Laravel's date/time wrapper) ───────────────────── */
    static const char *carbon_parents[] = {"DateTimeImmutable", NULL};
    REG_TYPE("Carbon.Carbon", "Carbon", false, carbon_parents);
    REG_TYPE("Carbon.CarbonImmutable", "CarbonImmutable", false, carbon_parents);
    {
        const char *c = "Carbon.Carbon";
        const CBMType *self = cbm_type_named(arena, "Carbon.Carbon");
        REG_METHOD(c, "addDay", self);
        REG_METHOD(c, "addDays", self);
        REG_METHOD(c, "subDay", self);
        REG_METHOD(c, "addMonth", self);
        REG_METHOD(c, "addYear", self);
        REG_METHOD(c, "startOfDay", self);
        REG_METHOD(c, "endOfDay", self);
        REG_METHOD(c, "format", cbm_type_builtin(arena, "string"));
        REG_METHOD(c, "toDateString", cbm_type_builtin(arena, "string"));
        REG_METHOD(c, "toDateTimeString", cbm_type_builtin(arena, "string"));
        REG_METHOD(c, "diffInDays", cbm_type_builtin(arena, "int"));
        REG_METHOD(c, "diffInHours", cbm_type_builtin(arena, "int"));
        REG_METHOD(c, "diffInMinutes", cbm_type_builtin(arena, "int"));
        REG_METHOD(c, "now", self);
        REG_METHOD(c, "today", self);
        REG_METHOD(c, "yesterday", self);
        REG_METHOD(c, "tomorrow", self);
        REG_METHOD(c, "parse", self);
    }

    /* ── PSR-7 / PSR-15 extras (used by Symfony stack) ──────────── */
    REG_METHOD("Psr.Http.Message.RequestInterface", "withMethod",
               cbm_type_named(arena, "Psr.Http.Message.RequestInterface"));
    REG_METHOD("Psr.Http.Message.RequestInterface", "withUri",
               cbm_type_named(arena, "Psr.Http.Message.RequestInterface"));
    REG_METHOD("Psr.Http.Message.ResponseInterface", "withStatus",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));
    REG_METHOD("Psr.Http.Message.ResponseInterface", "withHeader",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));

    /* ── Symfony Console ─────────────────────────────────────── */
    static const char *symfony_console_io_parents[] = {NULL};
    REG_TYPE("Symfony.Component.Console.Input.InputInterface", "InputInterface", true,
             symfony_console_io_parents);
    REG_TYPE("Symfony.Component.Console.Output.OutputInterface", "OutputInterface", true,
             symfony_console_io_parents);
    REG_TYPE("Symfony.Component.Console.Style.StyleInterface", "StyleInterface", true,
             symfony_console_io_parents);
    REG_TYPE("Symfony.Component.Console.Style.SymfonyStyle", "SymfonyStyle", false,
             symfony_console_io_parents);
    REG_METHOD("Symfony.Component.Console.Input.InputInterface", "getArgument", MIXED);
    REG_METHOD("Symfony.Component.Console.Input.InputInterface", "getOption", MIXED);
    REG_METHOD("Symfony.Component.Console.Input.InputInterface", "hasOption",
               cbm_type_builtin(arena, "bool"));
    REG_METHOD("Symfony.Component.Console.Output.OutputInterface", "writeln",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("Symfony.Component.Console.Output.OutputInterface", "write",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("Symfony.Component.Console.Style.SymfonyStyle", "success",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("Symfony.Component.Console.Style.SymfonyStyle", "error",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("Symfony.Component.Console.Style.SymfonyStyle", "info",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("Symfony.Component.Console.Style.SymfonyStyle", "warning",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("Symfony.Component.Console.Style.SymfonyStyle", "ask",
               cbm_type_builtin(arena, "string"));
    REG_METHOD("Symfony.Component.Console.Style.SymfonyStyle", "confirm",
               cbm_type_builtin(arena, "bool"));

    /* ── Doctrine ORM ───────────────────────────────────────── */
    static const char *doctrine_parents[] = {NULL};
    REG_TYPE("Doctrine.ORM.EntityManagerInterface", "EntityManagerInterface", true,
             doctrine_parents);
    REG_TYPE("Doctrine.ORM.EntityManager", "EntityManager", false, doctrine_parents);
    REG_TYPE("Doctrine.ORM.QueryBuilder", "QueryBuilder", false, doctrine_parents);
    REG_TYPE("Doctrine.ORM.Query", "Query", false, doctrine_parents);
    REG_TYPE("Doctrine.ORM.EntityRepository", "EntityRepository", false, doctrine_parents);
    REG_METHOD("Doctrine.ORM.EntityManagerInterface", "find", MIXED);
    REG_METHOD("Doctrine.ORM.EntityManagerInterface", "persist", cbm_type_builtin(arena, "void"));
    REG_METHOD("Doctrine.ORM.EntityManagerInterface", "remove", cbm_type_builtin(arena, "void"));
    REG_METHOD("Doctrine.ORM.EntityManagerInterface", "flush", cbm_type_builtin(arena, "void"));
    REG_METHOD("Doctrine.ORM.EntityManagerInterface", "createQuery",
               cbm_type_named(arena, "Doctrine.ORM.Query"));
    REG_METHOD("Doctrine.ORM.EntityManagerInterface", "createQueryBuilder",
               cbm_type_named(arena, "Doctrine.ORM.QueryBuilder"));
    REG_METHOD("Doctrine.ORM.EntityManagerInterface", "getRepository",
               cbm_type_named(arena, "Doctrine.ORM.EntityRepository"));
    {
        const char *qb = "Doctrine.ORM.QueryBuilder";
        const CBMType *self = cbm_type_named(arena, "Doctrine.ORM.QueryBuilder");
        REG_METHOD(qb, "select", self);
        REG_METHOD(qb, "from", self);
        REG_METHOD(qb, "where", self);
        REG_METHOD(qb, "andWhere", self);
        REG_METHOD(qb, "orWhere", self);
        REG_METHOD(qb, "join", self);
        REG_METHOD(qb, "leftJoin", self);
        REG_METHOD(qb, "innerJoin", self);
        REG_METHOD(qb, "orderBy", self);
        REG_METHOD(qb, "groupBy", self);
        REG_METHOD(qb, "setParameter", self);
        REG_METHOD(qb, "setMaxResults", self);
        REG_METHOD(qb, "setFirstResult", self);
        REG_METHOD(qb, "getQuery", cbm_type_named(arena, "Doctrine.ORM.Query"));
    }
    REG_METHOD("Doctrine.ORM.Query", "getResult", cbm_type_builtin(arena, "array"));
    REG_METHOD("Doctrine.ORM.Query", "getOneOrNullResult", MIXED);
    REG_METHOD("Doctrine.ORM.Query", "execute", cbm_type_builtin(arena, "array"));
    REG_METHOD("Doctrine.ORM.EntityRepository", "find", MIXED);
    REG_METHOD("Doctrine.ORM.EntityRepository", "findAll", cbm_type_builtin(arena, "array"));
    REG_METHOD("Doctrine.ORM.EntityRepository", "findBy", cbm_type_builtin(arena, "array"));
    REG_METHOD("Doctrine.ORM.EntityRepository", "findOneBy", MIXED);

    /* ── Guzzle HTTP ─────────────────────────────────────────── */
    static const char *guzzle_parents[] = {NULL};
    REG_TYPE("GuzzleHttp.Client", "Client", false, guzzle_parents);
    REG_TYPE("GuzzleHttp.ClientInterface", "ClientInterface", true, guzzle_parents);
    REG_METHOD("GuzzleHttp.ClientInterface", "request",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));
    REG_METHOD("GuzzleHttp.ClientInterface", "send",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));
    REG_METHOD("GuzzleHttp.ClientInterface", "sendAsync", MIXED);
    REG_METHOD("GuzzleHttp.Client", "request",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));
    REG_METHOD("GuzzleHttp.Client", "get",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));
    REG_METHOD("GuzzleHttp.Client", "post",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));
    REG_METHOD("GuzzleHttp.Client", "put",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));
    REG_METHOD("GuzzleHttp.Client", "delete",
               cbm_type_named(arena, "Psr.Http.Message.ResponseInterface"));

    /* ── Twig ─────────────────────────────────────────────── */
    static const char *twig_parents[] = {NULL};
    REG_TYPE("Twig.Environment", "Environment", false, twig_parents);
    REG_TYPE("Twig.TemplateWrapper", "TemplateWrapper", false, twig_parents);
    REG_METHOD("Twig.Environment", "render", cbm_type_builtin(arena, "string"));
    REG_METHOD("Twig.Environment", "load",
               cbm_type_named(arena, "Twig.TemplateWrapper"));
    REG_METHOD("Twig.Environment", "createTemplate",
               cbm_type_named(arena, "Twig.TemplateWrapper"));
    REG_METHOD("Twig.TemplateWrapper", "render", cbm_type_builtin(arena, "string"));
    REG_METHOD("Twig.TemplateWrapper", "renderBlock", cbm_type_builtin(arena, "string"));

    /* ── Eloquent Model magic-static methods (forwarded to Builder) ─ *
     *
     * Eloquent: every Model subclass has access to all Builder methods
     * via ::__callStatic. We register the full Builder method set as
     * static methods on Model so chains like User::where()->first()
     * resolve immediately. */
    {
        const char *em = "Illuminate.Database.Eloquent.Model";
        const CBMType *eb = cbm_type_named(arena, "Illuminate.Database.Eloquent.Builder");
        const CBMType *coll = cbm_type_named(arena, "Illuminate.Database.Eloquent.Collection");
        const CBMType *self = cbm_type_named(arena, "Illuminate.Database.Eloquent.Model");
        REG_METHOD(em, "whereIn", eb);
        REG_METHOD(em, "whereNotNull", eb);
        REG_METHOD(em, "whereHas", eb);
        REG_METHOD(em, "orderBy", eb);
        REG_METHOD(em, "limit", eb);
        REG_METHOD(em, "take", eb);
        REG_METHOD(em, "select", eb);
        REG_METHOD(em, "get", coll);
        REG_METHOD(em, "first", self);
        REG_METHOD(em, "firstOrFail", self);
        REG_METHOD(em, "firstOrCreate", self);
    }

    /* (Generator already registered in the SPL section above.) */

    /* ── Symfony Cache ─────────────────────────────────────── */
    static const char *cache_parents[] = {NULL};
    REG_TYPE("Symfony.Contracts.Cache.CacheInterface", "CacheInterface", true, cache_parents);
    REG_TYPE("Symfony.Contracts.Cache.ItemInterface", "ItemInterface", true, cache_parents);
    REG_TYPE("Psr.Cache.CacheItemPoolInterface", "CacheItemPoolInterface", true, cache_parents);
    REG_TYPE("Psr.Cache.CacheItemInterface", "CacheItemInterface", true, cache_parents);
    REG_METHOD("Symfony.Contracts.Cache.CacheInterface", "get", MIXED);
    REG_METHOD("Symfony.Contracts.Cache.CacheInterface", "delete", cbm_type_builtin(arena, "bool"));
    REG_METHOD("Symfony.Contracts.Cache.ItemInterface", "set",
               cbm_type_named(arena, "Symfony.Contracts.Cache.ItemInterface"));
    REG_METHOD("Symfony.Contracts.Cache.ItemInterface", "expiresAfter",
               cbm_type_named(arena, "Symfony.Contracts.Cache.ItemInterface"));
    REG_METHOD("Symfony.Contracts.Cache.ItemInterface", "get", MIXED);
    REG_METHOD("Psr.Cache.CacheItemPoolInterface", "getItem",
               cbm_type_named(arena, "Psr.Cache.CacheItemInterface"));
    REG_METHOD("Psr.Cache.CacheItemPoolInterface", "save",
               cbm_type_builtin(arena, "bool"));
    REG_METHOD("Psr.Cache.CacheItemInterface", "get", MIXED);
    REG_METHOD("Psr.Cache.CacheItemInterface", "set",
               cbm_type_named(arena, "Psr.Cache.CacheItemInterface"));

    /* ── Symfony EventDispatcher ────────────────────────────── */
    static const char *event_parents[] = {NULL};
    REG_TYPE("Symfony.Contracts.EventDispatcher.EventDispatcherInterface",
             "EventDispatcherInterface", true, event_parents);
    REG_TYPE("Symfony.Contracts.EventDispatcher.Event", "Event", false, event_parents);
    REG_METHOD("Symfony.Contracts.EventDispatcher.EventDispatcherInterface", "dispatch", MIXED);

    /* ── Symfony Mailer ─────────────────────────────────────── */
    static const char *mailer_parents[] = {NULL};
    REG_TYPE("Symfony.Component.Mailer.MailerInterface", "MailerInterface", true, mailer_parents);
    REG_TYPE("Symfony.Component.Mime.Email", "Email", false, mailer_parents);
    REG_METHOD("Symfony.Component.Mailer.MailerInterface", "send",
               cbm_type_builtin(arena, "void"));
    {
        const char *e = "Symfony.Component.Mime.Email";
        const CBMType *self = cbm_type_named(arena, "Symfony.Component.Mime.Email");
        REG_METHOD(e, "from", self);
        REG_METHOD(e, "to", self);
        REG_METHOD(e, "cc", self);
        REG_METHOD(e, "bcc", self);
        REG_METHOD(e, "subject", self);
        REG_METHOD(e, "text", self);
        REG_METHOD(e, "html", self);
        REG_METHOD(e, "attachFromPath", self);
    }

    /* ── Symfony Validator ─────────────────────────────────── */
    static const char *validator_parents[] = {NULL};
    REG_TYPE("Symfony.Component.Validator.Validator.ValidatorInterface", "ValidatorInterface",
             true, validator_parents);
    REG_TYPE("Symfony.Component.Validator.ConstraintViolationListInterface",
             "ConstraintViolationListInterface", true, validator_parents);
    REG_METHOD("Symfony.Component.Validator.Validator.ValidatorInterface", "validate",
               cbm_type_named(arena,
                              "Symfony.Component.Validator.ConstraintViolationListInterface"));
    REG_METHOD("Symfony.Component.Validator.ConstraintViolationListInterface", "count",
               cbm_type_builtin(arena, "int"));

    /* ── Laravel HTTP Request / Response ──────────────────── */
    static const char *laravel_request_parents[] = {"Symfony.Component.HttpFoundation.Request",
                                                     NULL};
    REG_TYPE("Illuminate.Http.Request", "Request", false, laravel_request_parents);
    REG_TYPE("Illuminate.Http.Response", "Response", false, laravel_request_parents);
    REG_TYPE("Illuminate.Http.JsonResponse", "JsonResponse", false, laravel_request_parents);
    REG_METHOD("Illuminate.Http.Request", "input", MIXED);
    REG_METHOD("Illuminate.Http.Request", "query", MIXED);
    REG_METHOD("Illuminate.Http.Request", "all", cbm_type_builtin(arena, "array"));
    REG_METHOD("Illuminate.Http.Request", "user", MIXED);
    REG_METHOD("Illuminate.Http.Request", "validate", cbm_type_builtin(arena, "array"));
    REG_METHOD("Illuminate.Http.Response", "header",
               cbm_type_named(arena, "Illuminate.Http.Response"));
    REG_METHOD("Illuminate.Http.Response", "withHeaders",
               cbm_type_named(arena, "Illuminate.Http.Response"));
    REG_METHOD("Illuminate.Http.JsonResponse", "header",
               cbm_type_named(arena, "Illuminate.Http.JsonResponse"));

    /* ── Laravel Auth ─────────────────────────────────────── */
    static const char *auth_parents[] = {NULL};
    REG_TYPE("Illuminate.Contracts.Auth.Authenticatable", "Authenticatable", true,
             auth_parents);
    REG_TYPE("Illuminate.Contracts.Auth.Guard", "Guard", true, auth_parents);
    REG_METHOD("Illuminate.Contracts.Auth.Authenticatable", "getAuthIdentifier", MIXED);
    REG_METHOD("Illuminate.Contracts.Auth.Authenticatable", "getAuthPassword",
               cbm_type_builtin(arena, "string"));
    REG_METHOD("Illuminate.Contracts.Auth.Guard", "user",
               cbm_type_named(arena, "Illuminate.Contracts.Auth.Authenticatable"));
    REG_METHOD("Illuminate.Contracts.Auth.Guard", "check", cbm_type_builtin(arena, "bool"));
    REG_METHOD("Illuminate.Contracts.Auth.Guard", "id", MIXED);

    /* ── Laravel Session / View ──────────────────────────── */
    static const char *session_parents[] = {NULL};
    REG_TYPE("Illuminate.Contracts.Session.Session", "Session", true, session_parents);
    REG_METHOD("Illuminate.Contracts.Session.Session", "get", MIXED);
    REG_METHOD("Illuminate.Contracts.Session.Session", "put", cbm_type_builtin(arena, "void"));
    REG_METHOD("Illuminate.Contracts.Session.Session", "has", cbm_type_builtin(arena, "bool"));
    REG_METHOD("Illuminate.Contracts.Session.Session", "flash", cbm_type_builtin(arena, "void"));

    REG_TYPE("Illuminate.View.View", "View", false, session_parents);
    REG_METHOD("Illuminate.View.View", "render", cbm_type_builtin(arena, "string"));
    REG_METHOD("Illuminate.View.View", "with",
               cbm_type_named(arena, "Illuminate.View.View"));

    /* ── ReactPHP / Promise ───────────────────────────────── */
    static const char *promise_parents[] = {NULL};
    REG_TYPE("React.Promise.PromiseInterface", "PromiseInterface", true, promise_parents);
    REG_TYPE("GuzzleHttp.Promise.PromiseInterface", "PromiseInterface", true, promise_parents);
    REG_METHOD("React.Promise.PromiseInterface", "then",
               cbm_type_named(arena, "React.Promise.PromiseInterface"));
    REG_METHOD("React.Promise.PromiseInterface", "catch",
               cbm_type_named(arena, "React.Promise.PromiseInterface"));
    REG_METHOD("React.Promise.PromiseInterface", "finally",
               cbm_type_named(arena, "React.Promise.PromiseInterface"));
    REG_METHOD("GuzzleHttp.Promise.PromiseInterface", "then",
               cbm_type_named(arena, "GuzzleHttp.Promise.PromiseInterface"));

    /* ── Monolog (popular logger) ───────────────────────── */
    static const char *monolog_parents[] = {"Psr.Log.LoggerInterface", NULL};
    REG_TYPE("Monolog.Logger", "Logger", false, monolog_parents);
    /* Logger inherits info/warning/error/etc. methods from PSR LoggerInterface. */
    REG_METHOD("Monolog.Logger", "pushHandler",
               cbm_type_named(arena, "Monolog.Logger"));
    REG_METHOD("Monolog.Logger", "pushProcessor",
               cbm_type_named(arena, "Monolog.Logger"));

    /* ── Reflection API ──────────────────────────────────── */
    static const char *reflection_parents[] = {NULL};
    REG_TYPE("ReflectionClass", "ReflectionClass", false, reflection_parents);
    REG_TYPE("ReflectionMethod", "ReflectionMethod", false, reflection_parents);
    REG_TYPE("ReflectionProperty", "ReflectionProperty", false, reflection_parents);
    REG_TYPE("ReflectionFunction", "ReflectionFunction", false, reflection_parents);
    REG_METHOD("ReflectionClass", "getName", cbm_type_builtin(arena, "string"));
    REG_METHOD("ReflectionClass", "getMethods", cbm_type_builtin(arena, "array"));
    REG_METHOD("ReflectionClass", "getMethod",
               cbm_type_named(arena, "ReflectionMethod"));
    REG_METHOD("ReflectionClass", "newInstance", MIXED);
    REG_METHOD("ReflectionClass", "newInstanceArgs", MIXED);
    REG_METHOD("ReflectionMethod", "invoke", MIXED);
    REG_METHOD("ReflectionMethod", "invokeArgs", MIXED);
    REG_METHOD("ReflectionMethod", "getName", cbm_type_builtin(arena, "string"));
    REG_METHOD("ReflectionProperty", "getValue", MIXED);
    REG_METHOD("ReflectionProperty", "setValue", cbm_type_builtin(arena, "void"));
}

#undef REG_TYPE
#undef REG_METHOD
#undef MIXED
