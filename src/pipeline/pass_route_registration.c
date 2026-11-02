/*
 * pass_route_registration.c -- is this call a real route registration?
 *
 * A route verb with a slash-leading string says little: `client.get('/x')`,
 * `store.Delete("/foo")` and `title.delete('/')` all have that shape. A real
 * registration is told by what the code spells, in this order:
 *
 *   1. a form only a framework has: a Python decorator, `HandleFunc`, `MapGet`,
 *      `add_url_rule`, `Route::get`;
 *   2. a route method on a receiver the file binds to a router: a factory call
 *      (`const app = express()`), a parameter's declared type (`r chi.Router`),
 *      or a group derived from one (`v1 := r.Group("/v1")`);
 *   3. a receiver the code itself calls a router (`router`, `httpmux`), or an
 *      `app`, given both a literal path and a handler.
 *
 * Each needs the path literal in the path position and, for a route verb, a
 * handler argument or a chained continuation. The answer is a pure function of
 * one file's extraction result and bytes. The binding set is built once per
 * file, on its first candidate, in one pass over its calls and definitions, and
 * lives in thread-local state between begin and end: a worker resolves one file
 * at a time. A file with no candidate pays nothing.
 */
#include "foundation/compat.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/shared_resolver.h"
#include "foundation/str_util.h"
#include "graph_buffer/graph_buffer.h"
#include "service_patterns.h"
#include <yyjson/yyjson.h>

#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const CBMFileResult *result;
    const char *path; /* absolute path, for bytes extraction did not retain */
    CBMLanguage language;
    const char *source;
    size_t source_length;
    char *owned_source;
    bool built;
    bool spells_sinatra;
    bool spells_rails_routes;
    CBMArena *arena;       /* on the heap: an arena is 4 KB, and thread-locals shrink every stack */
    CBMHashTable *routers; /* receiver names the file binds to a router */
} route_file_t;

static CBM_TLS route_file_t tl_route_file;
static CBM_TLS bool tl_route_file_active;

static bool name_in(const char *name, size_t length, const char *const *names) {
    for (; *names; names++) {
        if (strlen(*names) == length && memcmp(*names, name, length) == 0)
            return true;
    }
    return false;
}

/* The factories of the frameworks route_reg_libraries names, spelled as a call. */
static bool route_factory(const char *callee, size_t length) {
    static const char *const exact[] = {
        /* JavaScript / TypeScript */
        "express", "express.Router", "fastify", "Fastify", "Hono", "Elysia", "polka",
        "restify.createServer",
        /* Python */
        "Flask", "flask.Flask", "Blueprint", "flask.Blueprint", "FastAPI", "fastapi.FastAPI",
        "APIRouter", "fastapi.APIRouter", "Starlette", "Sanic", "Quart", "Bottle", "bottle.Bottle",
        "web.Application", "APIFlask",
        /* Go */
        "gin.Default", "gin.New", "echo.New", "fiber.New", "httprouter.New", "route.New",
        /* Rust */
        "App::new", "rocket::build", "rocket::ignite", "web::scope",
        /* JVM */
        "Javalin.create", "RoutingDsl.fromComponents", "routingDsl", "newRoutingDsl",
        "Router.router",
        /* PHP */
        "AppFactory::create", NULL};
    if (name_in(callee, length, exact))
        return true;
    /* `const app = require('fastify')({ logger: true })` */
    static const char *const required[] = {"require('express')", "require(\"express\")",
                                           "require('fastify')", "require(\"fastify\")", NULL};
    if (name_in(callee, length, required))
        return true;
    /* Whatever the framework: `mux.NewRouter()`, `new KoaRouter()`, `Router::new()`. */
    static const char *const tails[] = {"Router", "KoaRouter",    "NewRouter", "NewServeMux",
                                        "NewMux", "createRouter", NULL};
    size_t start = length;
    while (start > 0 && (isalnum((unsigned char)callee[start - 1]) || callee[start - 1] == '_'))
        start--;
    if (name_in(callee + start, length - start, tails))
        return true;
    static const char rust_router[] = "Router::new";
    size_t rust_length = sizeof(rust_router) - 1;
    return length >= rust_length &&
           memcmp(callee + length - rust_length, rust_router, rust_length) == 0;
}

/* A declared type that is a router: its last segment, or the whole spelling
 * where the last segment alone says nothing (`echo.Group`, `gin.Engine`). */
static bool route_type(const char *type) {
    char plain[CBM_SZ_128];
    size_t n = 0;
    for (const char *p = type; *p && *p != '<' && *p != '['; p++) {
        if (isalnum((unsigned char)*p) || *p == '_' || *p == '.' || *p == ':' || *p == '\\') {
            if (n + 1 >= sizeof(plain))
                return false;
            plain[n++] = *p;
        }
    }
    plain[n] = '\0';
    static const char *const exact[] = {"gin.Engine",
                                        "gin.RouterGroup",
                                        "gin.IRouter",
                                        "gin.IRoutes",
                                        "echo.Echo",
                                        "echo.Group",
                                        "fiber.App",
                                        "restful.WebService",
                                        "web.ServiceConfig",
                                        "Express",
                                        "express.Express",
                                        "express.Application",
                                        "FastifyInstance",
                                        "Hono",
                                        "Flask",
                                        "flask.Flask",
                                        "Blueprint",
                                        "flask.Blueprint",
                                        "FastAPI",
                                        "fastapi.FastAPI",
                                        "APIRouter",
                                        "fastapi.APIRouter",
                                        "Javalin",
                                        "Slim\\App",
                                        NULL};
    if (name_in(plain, n, exact))
        return true;
    size_t start = n;
    while (start > 0 && plain[start - 1] != '.' && plain[start - 1] != ':' &&
           plain[start - 1] != '\\')
        start--;
    static const char *const tails[] = {"Router", "ServeMux", "Mux", NULL};
    return name_in(plain + start, n - start, tails);
}

/* Copy without the whitespace a chained call keeps in its source spelling. */
static size_t route_squeeze(const char *text, char *out, size_t cap) {
    size_t n = 0;
    for (const char *p = text; *p; p++) {
        if (isspace((unsigned char)*p))
            continue;
        if (n + 1 >= cap)
            return 0;
        out[n++] = *p;
    }
    out[n] = '\0';
    return n;
}

/* The last name of a member path: `this.app` -> `app`, `$app` -> `app`. */
static const char *route_tail(const char *receiver, size_t *length) {
    size_t end = strlen(receiver);
    size_t start = end;
    while (start > 0) {
        char c = receiver[start - 1];
        if (c == '.' || c == ':' || c == '>' || c == '$' || c == '@')
            break;
        start--;
    }
    *length = end - start;
    return receiver + start;
}

static void route_bind(route_file_t *file, const char *name, size_t length) {
    while (length > 0 && (*name == '$' || *name == '@')) {
        name++;
        length--;
    }
    if (length == 0 || length >= CBM_SZ_128)
        return;
    char *copy = cbm_arena_strndup(file->arena, name, length);
    if (copy)
        cbm_ht_set(file->routers, copy, copy);
}

static size_t route_skip_blanks(const char *src, size_t at) {
    while (at > 0 && (src[at - 1] == ' ' || src[at - 1] == '\t'))
        at--;
    return at;
}

static bool route_name_char(char c) {
    return isalnum((unsigned char)c) || c == '_' || c == '.' || c == '$' || c == '@' || c == '#';
}

/* The names a call's value is assigned to, read back from its site. The site is
 * an AST node's span, so a comment or a string can never pose as the binding.
 * `const app: Express = express()`, `app := gin.New()`, `this.app = express()`,
 * and both names of `var app = module.exports = express()`. */
static void route_bind_assignment(route_file_t *file, const CBMCall *call) {
    if (!file->source || call->source_origin != CBM_SOURCE_ORIGIN_RAW ||
        call->site_start_byte == 0 || call->site_start_byte > file->source_length)
        return;
    const char *src = file->source;
    size_t at = route_skip_blanks(src, call->site_start_byte);
    while (at > 0 && src[at - 1] == '=') {
        at--;
        if (at > 0 && src[at - 1] == ':')
            at--; /* Go's `:=` */
        else if (at > 0 && strchr("!<>=+-*/%&|^", src[at - 1]))
            return; /* a comparison or a compound assignment binds nothing */
        at = route_skip_blanks(src, at);
        size_t end = at;
        while (at > 0 && route_name_char(src[at - 1]))
            at--;
        /* `const app: Express`: the name precedes the annotation's colon. */
        size_t colon = route_skip_blanks(src, at);
        if (colon > 0 && src[colon - 1] == ':' && (colon < 2 || src[colon - 2] != ':')) {
            end = route_skip_blanks(src, colon - 1);
            at = end;
            while (at > 0 && route_name_char(src[at - 1]))
                at--;
        }
        size_t start = end;
        while (start > at && src[start - 1] != '.')
            start--;
        route_bind(file, src + start, end - start);
        at = route_skip_blanks(src, at);
    }
}

static bool route_named_receiver(const char *tail, size_t length);

static bool route_derives(const char *callee, size_t length, const route_file_t *file) {
    static const char *const groups[] = {"Group", "group", "Subrouter", "PathPrefix",
                                         "With",  "scope", NULL};
    size_t dot = length;
    while (dot > 0 && callee[dot - 1] != '.')
        dot--;
    if (dot == 0 || !name_in(callee + dot, length - dot, groups))
        return false;
    char receiver[CBM_SZ_128];
    size_t size = dot - 1;
    if (size == 0 || size >= sizeof(receiver))
        return false;
    memcpy(receiver, callee, size);
    receiver[size] = '\0';
    size_t tail_length = 0;
    const char *tail = route_tail(receiver, &tail_length);
    return cbm_ht_has(file->routers, tail) || route_named_receiver(tail, tail_length);
}

/* Go: `func(r chi.Router) {...}` passed as an argument declares its router. */
static void route_bind_func_literal(route_file_t *file, const char *expr) {
    static const char open[] = "func(";
    if (!expr || strncmp(expr, open, sizeof(open) - 1) != 0)
        return;
    const char *name = expr + sizeof(open) - 1;
    const char *name_end = strchr(name, ' ');
    const char *end = name + strcspn(name, ",)");
    if (!name_end || name_end >= end || end - name_end >= CBM_SZ_128)
        return;
    char type[CBM_SZ_128];
    snprintf(type, sizeof(type), "%.*s", (int)(end - name_end - 1), name_end + 1);
    if (route_type(type))
        route_bind(file, name, (size_t)(name_end - name));
}

/* Does the file spell this anywhere? */
static bool route_file_spells(const route_file_t *file, const char *word) {
    size_t length = strlen(word);
    const char *p = file->source;
    const char *end = p ? p + file->source_length : NULL;
    while (p && length && end - p >= (ptrdiff_t)length) {
        p = memchr(p, word[0], (size_t)(end - p) - length + 1);
        if (!p)
            return false;
        if (memcmp(p, word, length) == 0)
            return true;
        p++;
    }
    return false;
}

/* One pass over the file's calls, in source order so a group of a group binds,
 * and one over its definitions' declared parameters. */
static void route_file_build(route_file_t *file) {
    file->built = true;
    file->arena = cbm_alloc(CBM_MEM_CLASS_EXTRACT, sizeof(*file->arena));
    if (file->arena)
        cbm_arena_init_lazy(file->arena, CBM_SZ_4K);
    file->routers = file->arena ? cbm_ht_create(16) : NULL;
    if (!file->routers)
        return;
    const CBMFileResult *result = file->result;
    file->source = result->source;
    file->source_length = result->source_len > 0 ? (size_t)result->source_len : 0;
    if (!file->source && file->path) {
        file->owned_source = cbm_shared_resolver_load_source(file->path, &file->source_length);
        file->source = file->owned_source;
    }
    /* A framework's factory under the file's own import name: `import e from 'express'`. */
    CBMHashTable *imported = NULL;
    for (int i = 0; i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (import->local_name && import->module_path &&
            cbm_service_pattern_match(import->module_path) == CBM_SVC_ROUTE_REG) {
            imported = imported ? imported : cbm_ht_create(8);
            if (imported)
                cbm_ht_set(imported, import->local_name, (void *)import);
        }
    }
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (!call->callee_name)
            continue;
        char callee[CBM_SZ_256];
        size_t length = route_squeeze(call->callee_name, callee, sizeof(callee));
        if (length && (route_factory(callee, length) || route_derives(callee, length, file) ||
                       (imported && cbm_ht_has(imported, callee))))
            route_bind_assignment(file, call);
        if (file->language != CBM_LANG_GO)
            continue;
        /* Handlers' typed router parameters. */
        for (int a = 0; a < call->arg_count; a++)
            route_bind_func_literal(file, call->args[a].expr);
    }
    if (imported)
        cbm_ht_free(imported);
    if (file->language == CBM_LANG_RUBY)
        file->spells_sinatra = route_file_spells(file, "Sinatra");
    if (file->language == CBM_LANG_RUBY)
        file->spells_rails_routes = route_file_spells(file, "routes.draw");
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        /* param_types drops builtins and repeats; only the ordered signature
         * lines up with the names, and only when it has one type per name. */
        if (!def->param_names || !def->signature_param_types)
            continue;
        int names = 0;
        while (def->param_names[names])
            names++;
        if (names != def->signature_param_count)
            continue;
        for (int p = 0; p < names; p++) {
            if (route_type(def->signature_param_types[p]))
                route_bind(file, def->param_names[p], strlen(def->param_names[p]));
        }
    }
}

const char *cbm_pipeline_sql_lineage_mark(CBMLanguage language, const char *rel,
                                          const cbm_gbuf_node_t *target) {
    if (language != CBM_LANG_SQL || !target || !target->label ||
        !cbm_label_is_relation(target->label))
        return "";
    bool same_script = rel && target->file_path && strcmp(rel, target->file_path) == 0;
    return same_script || strcmp(target->label, "Model") == 0 ? ",\"via\":\"sql_relation\"" : "";
}

void cbm_route_registration_file_begin(const CBMFileResult *result, CBMLanguage language,
                                       const char *path) {
    tl_route_file = (route_file_t){.result = result, .path = path, .language = language};
    tl_route_file_active = result != NULL;
}

void cbm_route_registration_file_end(void) {
    if (tl_route_file_active && tl_route_file.built) {
        if (tl_route_file.routers)
            cbm_ht_free(tl_route_file.routers);
        if (tl_route_file.arena)
            cbm_arena_destroy(tl_route_file.arena);
        cbm_free(CBM_MEM_CLASS_EXTRACT, tl_route_file.arena);
        cbm_free(CBM_MEM_CLASS_EXTRACT, tl_route_file.owned_source);
    }
    tl_route_file = (route_file_t){0};
    tl_route_file_active = false;
}

static bool route_literal(const char *expr, const char *value) {
    if (expr && (expr[0] == '{' || expr[0] == '['))
        return false; /* a request-config object's url is no path argument */
    if (value && value[0])
        return true;
    /* A quoted string, or a JavaScript regex: `app.use(/^\/[a-z]oo/, fn)`. */
    return expr && (expr[0] == '"' || expr[0] == '\'' || expr[0] == '`' || expr[0] == '/');
}

/* The path is the first positional argument, or a keyword's slash-led value:
 * `m.Client.Get(ctx, "/x")` names no path in the path position. */
static bool route_path_in_position(const CBMCall *call) {
    for (int i = 0; i < call->arg_count; i++) {
        const CBMCallArg *arg = &call->args[i];
        if (arg->keyword) {
            if (arg->value && arg->value[0] == '/')
                return true;
        } else if (arg->index == 0 && (route_literal(arg->expr, arg->value) ||
                                       (arg->expr && call->first_string_arg &&
                                        strstr(arg->expr, call->first_string_arg)))) {
            /* The second form is a composed path: `baseUrl + "/product"`. */
            return true;
        }
    }
    return false;
}

static bool route_data_literal(const char *expr) {
    static const char *const words[] = {"true", "false", "null", "nil",       "None",
                                        "True", "False", "NULL", "undefined", NULL};
    if (!expr || !expr[0])
        return true;
    char c = expr[0];
    if (c == '"' || c == '\'' || c == '`' || c == '{' || c == '[' || c == '-' ||
        isdigit((unsigned char)c))
        return true;
    return name_in(expr, strlen(expr), words);
}

/* A handler: a later argument that holds code, not data. PHP spells it as data:
 * `$router->get('/x', 'Home@index')`, `[Home::class, 'index']`. */
static bool route_has_handler(const CBMCall *call, CBMLanguage language) {
    if (language == CBM_LANG_PHP)
        return call->arg_count >= 2;
    for (int i = 0; i < call->arg_count; i++) {
        const CBMCallArg *arg = &call->args[i];
        if (arg->keyword) {
            static const char *const names[] = {"view_func", "endpoint", "handler", "to", NULL};
            if (name_in(arg->keyword, strlen(arg->keyword), names))
                return true;
        } else if (arg->index >= 1 && !route_data_literal(arg->expr)) {
            return true;
        }
    }
    return false;
}

static bool route_decorates(const route_file_t *file, const CBMCall *call) {
    if (!file->source || call->source_origin != CBM_SOURCE_ORIGIN_RAW ||
        call->site_start_byte == 0 || call->site_start_byte > file->source_length)
        return false;
    return file->source[call->site_start_byte - 1] == '@';
}

/* `r.Get("/x").To(h)`, `router.route('/x').get(h)`, `app.post("/x")(h)`: the
 * handler follows the call. */
static bool route_chain_continues(const route_file_t *file, const CBMCall *call) {
    if (!file->source || call->source_origin != CBM_SOURCE_ORIGIN_RAW)
        return false;
    size_t at = call->site_end_byte;
    while (at < file->source_length && isspace((unsigned char)file->source[at]))
        at++;
    return at < file->source_length && (file->source[at] == '.' || file->source[at] == '(');
}

/* Is the call chained straight into this method? go-restful's `ws.GET("/x").To(h)`
 * and Play's `dsl.GET("/x").routingTo(h)` bind the handler there, and nothing but
 * a route builder has that next link. */
static bool route_next_link_is(const route_file_t *file, const CBMCall *call, const char *name) {
    if (!file->source || call->source_origin != CBM_SOURCE_ORIGIN_RAW)
        return false;
    size_t at = call->site_end_byte;
    while (at < file->source_length && isspace((unsigned char)file->source[at]))
        at++;
    if (at >= file->source_length || file->source[at] != '.')
        return false;
    at++;
    while (at < file->source_length && isspace((unsigned char)file->source[at]))
        at++;
    size_t length = strlen(name);
    return at + length < file->source_length && memcmp(file->source + at, name, length) == 0 &&
           file->source[at + length] == '(';
}

/* A method only a framework has: the spelling is the evidence. */
static bool route_unique_form(CBMLanguage language, const char *receiver, const char *verb,
                              size_t verb_length, bool handler) {
    static const char *const py[] = {"add_url_rule",
                                     "include_router",
                                     "register_blueprint",
                                     "add_api_route",
                                     "add_api_websocket_route",
                                     "add_route",
                                     "websocket_route",
                                     NULL};
    static const char *const dotnet[] = {"MapGet", "MapPost", "MapPut", "MapDelete", NULL};
    static const char *const go[] = {"Handle", "HandleFunc", NULL};
    switch (language) {
    case CBM_LANG_PYTHON:
        return name_in(verb, verb_length, py);
    case CBM_LANG_CSHARP:
        return name_in(verb, verb_length, dotnet);
    case CBM_LANG_GO:
        return handler && name_in(verb, verb_length, go);
    case CBM_LANG_PHP: {
        /* The Laravel facade, plain or fully named, and its fluent chains:
         * `Route::get`, `\\Illuminate\\Support\\Facades\\Route::get`,
         * `Route::middleware('auth')->post`. `Http::get` is a client. */
        const char *bar = strrchr(receiver, '\\');
        const char *name = bar ? bar + SKIP_ONE : receiver;
        return strcmp(name, "Route") == 0 || strncmp(name, "Route::", 7) == 0;
    }
    default:
        return false;
    }
}

/* What the code itself calls a router, an app, or by its framework's name. */
static bool route_named_receiver(const char *tail, size_t length) {
    static const char *const names[] = {"app", "App", NULL};
    if (name_in(tail, length, names))
        return true;
    char lower[CBM_SZ_64];
    if (length == 0 || length >= sizeof(lower))
        return false;
    for (size_t i = 0; i < length; i++)
        lower[i] = (char)tolower((unsigned char)tail[i]);
    lower[length] = '\0';
    if (strstr(lower, "client") || strstr(lower, "tmux") || strstr(lower, "demux"))
        return false;
    static const char *const words[] = {"router",  "routing", "mux", "express",
                                        "fastify", "hono",    "koa", NULL};
    for (const char *const *word = words; *word; word++) {
        if (strstr(lower, *word))
            return true;
    }
    return false;
}

/* `app.route('/x').get`, `r.With(mw).Get`: a chain of router builders. */
static bool route_chain_bound(const route_file_t *file, const char *receiver) {
    static const char *const links[] = {"Group",     "group",      "With",  "Use",   "use",
                                        "Subrouter", "PathPrefix", "Route", "route", NULL};
    const char *paren = strchr(receiver, '(');
    if (paren && route_factory(receiver, (size_t)(paren - receiver)))
        return true;
    char root[CBM_SZ_128];
    size_t root_length = strcspn(receiver, ".(");
    if (root_length == 0 || root_length >= sizeof(root) || receiver[root_length] != '.')
        return false;
    memcpy(root, receiver, root_length);
    root[root_length] = '\0';
    const char *key = root[0] == '$' ? root + 1 : root;
    if (!cbm_ht_has(file->routers, key))
        return false;
    /* Every link between the root and the verb must return a router. */
    const char *p = receiver + root_length;
    while (*p == '.') {
        p++;
        size_t name = strcspn(p, ".(");
        if (!name_in(p, name, links))
            return false;
        p += name;
        if (*p != '(')
            continue; /* Go's callee keeps only the names: `app.Group.Get` */
        int depth = 0;
        do {
            if (*p == '(')
                depth++;
            else if (*p == ')')
                depth--;
            p++;
        } while (*p && depth > 0);
    }
    return *p == '\0';
}

const char *cbm_route_registration_dsl_method(const CBMCall *call, CBMLanguage language) {
    static const struct {
        const char *verb, *method;
    } verbs[] = {
        {"get", "GET"}, {"post", "POST"}, {"put", "PUT"}, {"delete", "DELETE"}, {"patch", "PATCH"}};
    if (language != CBM_LANG_RUBY || !call || !call->callee_name)
        return NULL;
    for (size_t i = 0; i < sizeof(verbs) / sizeof(verbs[0]); i++) {
        if (strcmp(call->callee_name, verbs[i].verb) == 0)
            return verbs[i].method;
    }
    return NULL;
}

/* Ruby's route DSLs have no receiver. Sinatra: `get '/x' do ... end` in a file that
 * names the framework; the block is the handler, and a rack-test `get '/x'` has none.
 * Rails: `get '/x', to: 'items#index'` in the file that draws the routes. */
static bool route_ruby_dsl_form(const route_file_t *file, const CBMCall *call) {
    if (!cbm_route_registration_dsl_method(call, file->language))
        return false;
    if (file->spells_rails_routes) {
        for (int i = 0; i < call->arg_count; i++) {
            if (call->args[i].keyword && strcmp(call->args[i].keyword, "to") == 0)
                return true;
        }
    }
    if (!file->spells_sinatra || !file->source || call->source_origin != CBM_SOURCE_ORIGIN_RAW ||
        call->site_end_byte > file->source_length || call->site_end_byte < 3)
        return false;
    const char *end = file->source + call->site_end_byte;
    return end[-1] == '}' || memcmp(end - 3, "end", 3) == 0;
}

bool cbm_route_registration_real(const CBMCall *call) {
    route_file_t *file = &tl_route_file;
    if (!tl_route_file_active || !call || !call->callee_name)
        return false;
    char callee[CBM_SZ_1K];
    size_t length = route_squeeze(call->callee_name, callee, sizeof(callee));
    if (length == 0)
        return false;
    size_t verb_start = length;
    while (verb_start > 0 && callee[verb_start - 1] != '.' && callee[verb_start - 1] != ':')
        verb_start--;
    if (verb_start < 2) {
        if (file->language != CBM_LANG_RUBY || !route_path_in_position(call))
            return false; /* a bare call has no receiver to judge */
        if (!file->built)
            route_file_build(file);
        return route_ruby_dsl_form(file, call);
    }
    size_t receiver_end = verb_start - 1;
    if (callee[receiver_end] == ':' && receiver_end > 0 && callee[receiver_end - 1] == ':')
        receiver_end--;
    if (receiver_end == 0)
        return false;
    if (!cbm_service_pattern_route_method(callee))
        return false;
    const char *verb = callee + verb_start;
    size_t verb_length = length - verb_start;
    callee[receiver_end] = '\0';
    const char *receiver = callee;

    if (!route_path_in_position(call))
        return false;
    if (!file->built)
        route_file_build(file);
    bool handler = route_has_handler(call, file->language);
    if (route_unique_form(file->language, receiver, verb, verb_length, handler))
        return true;
    if (file->language == CBM_LANG_PYTHON && route_decorates(file, call))
        return true;
    if ((file->language == CBM_LANG_GO && route_next_link_is(file, call, "To")) ||
        route_next_link_is(file, call, "routingTo") ||
        route_next_link_is(file, call, "routingAsync"))
        return true;
    bool complete = handler || route_chain_continues(file, call);
    if (!complete)
        return false;
    if (file->routers && route_chain_bound(file, receiver))
        return true;
    if (strchr(receiver, '('))
        return false;
    size_t tail_length = 0;
    const char *tail = route_tail(receiver, &tail_length);
    if (file->routers && cbm_ht_has(file->routers, tail))
        return true;
    return handler && route_named_receiver(tail, tail_length);
}

void cbm_route_registration_note_handler(char *props, size_t props_size, const CBMCall *call,
                                         const char *handler_ref) {
    size_t length = strlen(props);
    if (!handler_ref || !handler_ref[0] || call->site_end_byte <= call->site_start_byte ||
        call->source_origin != CBM_SOURCE_ORIGIN_RAW || length == 0 || props[length - 1] != '}')
        return;
    char escaped[CBM_SZ_256];
    cbm_json_escape(escaped, sizeof(escaped), handler_ref);
    char tail[CBM_SZ_512];
    int written = snprintf(tail, sizeof(tail), ",\"handler_ref\":\"%s\",\"site\":[%u,%u]}", escaped,
                           call->site_start_byte, call->site_end_byte);
    if (written > 0 && (size_t)written < sizeof(tail) && length + (size_t)written < props_size)
        memcpy(props + length - 1, tail, (size_t)written + 1);
}

typedef struct {
    const char *file;
    const char *ref;
    const char *target;
    int64_t route;
    uint32_t start, end, target_start;
} route_pending_t;

typedef struct {
    route_pending_t *first;
    size_t count;
} route_run_t;

static int route_pending_order(const void *left, const void *right) {
    const route_pending_t *a = left, *b = right;
    int order = strcmp(a->file, b->file);
    if (!order && a->start != b->start)
        order = a->start < b->start ? -1 : 1;
    return order;
}

/* The last name of `a.b`, `a::b` or `a->b`. */
static const char *route_last_name(const char *text) {
    const char *last = text;
    for (const char *p = text; *p; p++) {
        if (*p == '.' || *p == ':' || *p == '>')
            last = p + 1;
    }
    return last;
}

/* The registration whose call span holds this occurrence, by binary search in
 * its file's run; spans of one file's calls do not partly overlap. */
static route_pending_t *route_pending_at(route_pending_t *run, size_t count, uint32_t start,
                                         uint32_t end) {
    size_t low = 0, high = count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (run[mid].start <= start)
            low = mid + 1;
        else
            high = mid;
    }
    return low > 0 && end <= run[low - 1].end ? &run[low - 1] : NULL;
}

/* Pending registrations of the graph's Routes: those with a noted handler. */
static bool route_pending_collect(const cbm_gbuf_t *graph, CBMArena *arena, route_pending_t **out,
                                  size_t *count) {
    const cbm_gbuf_node_t **routes = NULL;
    int route_count = 0;
    if (cbm_gbuf_find_by_label(graph, "Route", &routes, &route_count) != 0)
        return true;
    size_t capacity = 0;
    for (int r = 0; r < route_count; r++) {
        const cbm_gbuf_edge_t **edges = NULL;
        int edge_count = 0;
        if (cbm_gbuf_find_edges_by_target_type(graph, routes[r]->id, "CALLS", &edges,
                                               &edge_count) != 0)
            continue;
        for (int e = 0; e < edge_count; e++) {
            const char *props = edges[e]->properties_json;
            if (!props || !strstr(props, "\"handler_ref\""))
                continue;
            yyjson_doc *doc = yyjson_read(props, strlen(props), 0);
            yyjson_val *root = yyjson_doc_get_root(doc);
            const char *ref = yyjson_get_str(yyjson_obj_get(root, "handler_ref"));
            yyjson_val *site = yyjson_obj_get(root, "site");
            const cbm_gbuf_node_t *source = cbm_gbuf_find_by_id(graph, edges[e]->source_id);
            if (ref && yyjson_arr_size(site) == 2 && source && source->file_path) {
                if (*count == capacity) {
                    size_t grown = capacity ? capacity * 2 : CBM_SZ_64;
                    route_pending_t *larger = cbm_arena_alloc(arena, grown * sizeof(*larger));
                    if (!larger) {
                        yyjson_doc_free(doc);
                        return false;
                    }
                    if (*count)
                        memcpy(larger, *out, *count * sizeof(*larger));
                    *out = larger;
                    capacity = grown;
                }
                (*out)[(*count)++] =
                    (route_pending_t){.file = source->file_path,
                                      .ref = cbm_arena_strdup(arena, ref),
                                      .route = routes[r]->id,
                                      .start = (uint32_t)yyjson_get_uint(yyjson_arr_get(site, 0)),
                                      .end = (uint32_t)yyjson_get_uint(yyjson_arr_get(site, 1))};
            }
            yyjson_doc_free(doc);
        }
    }
    return true;
}

bool cbm_route_registration_link_handlers(cbm_gbuf_t *graph) {
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_SZ_4K);
    route_pending_t *pending = NULL;
    size_t count = 0;
    bool ok = route_pending_collect(graph, &arena, &pending, &count);
    if (ok && count > 0) {
        qsort(pending, count, sizeof(*pending), route_pending_order);
        /* file -> its run of the sorted rows */
        CBMHashTable *runs = cbm_ht_create((uint32_t)count);
        ok = runs != NULL;
        for (size_t i = 0; ok && i < count;) {
            size_t next = i + 1;
            while (next < count && !strcmp(pending[next].file, pending[i].file))
                next++;
            route_run_t *run = cbm_arena_alloc(&arena, sizeof(*run));
            ok = run != NULL;
            if (ok) {
                *run = (route_run_t){&pending[i], next - i};
                cbm_ht_set(runs, pending[i].file, run);
            }
            i = next;
        }
        size_t records = 0;
        const cbm_resolve_record_t *rows = ok ? cbm_gbuf_resolutions(graph, &records) : NULL;
        for (size_t i = 0; ok && i < records; i++) {
            const cbm_resolve_record_t *row = &rows[i];
            if (!row->emitted || !row->relation || !row->file_path || !row->spelling ||
                (strcmp(row->relation, "CALL_REFERENCE") && strcmp(row->relation, "USAGE")))
                continue;
            const route_run_t *run = cbm_ht_get(runs, row->file_path);
            if (!run)
                continue;
            route_pending_t *hit =
                route_pending_at(run->first, run->count, row->start_byte, row->end_byte);
            if (hit && !strcmp(route_last_name(row->spelling), route_last_name(hit->ref)) &&
                (!hit->target || row->start_byte > hit->target_start)) {
                hit->target = row->result.target;
                hit->target_start = row->start_byte;
            }
        }
        for (size_t i = 0; ok && i < count; i++) {
            const cbm_gbuf_node_t *handler =
                pending[i].target ? cbm_gbuf_find_by_qn(graph, pending[i].target) : NULL;
            if (!handler)
                continue;
            char escaped[CBM_SZ_512];
            char props[CBM_SZ_1K];
            cbm_json_escape(escaped, sizeof(escaped), handler->qualified_name);
            snprintf(props, sizeof(props), "{\"handler\":\"%s\"}", escaped);
            cbm_gbuf_insert_edge(graph, handler->id, pending[i].route, "HANDLES", props);
        }
        cbm_ht_free(runs);
    }
    cbm_arena_destroy(&arena);
    return ok;
}
