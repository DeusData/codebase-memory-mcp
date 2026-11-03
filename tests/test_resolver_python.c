#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"
#include <stdlib.h>

typedef struct {
    const char *path, *text;
} python_proof_file_t;

static int python_proof_edges(const python_proof_file_t *files, size_t count, const char *target,
                              const char *name, const char *label) {
    const char *root = th_mktempdir("cbm_python_proof");
    if (!root)
        return -1;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755) && cbm_mkdir_p(TH_PATH(repo, "lib"), 0755);
    for (size_t i = 0; ok && i < count; i++)
        ok = th_write_file(TH_PATH(repo, files[i].path), files[i].text) == 0;
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "python-proof")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (store &&
        sqlite3_prepare_v2(
            cbm_store_get_db(store),
            "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND s.file_path='main.py' "
            "AND t.file_path=?1 AND t.name=?2 AND t.label=?3",
            -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, target, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, name, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 3, label, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            edges = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return edges;
}

TEST(resolver_python_reexport_chain_preserves_actual_declaration) {
    const python_proof_file_t files[] = {
        {"lib/actual.py", "def real():\n    return 1\n"},
        {"lib/mid.py", "from lib.actual import real as alias\n"},
        {"lib/front.py", "from lib.mid import alias as endpoint\n"},
        {"main.py", "from lib.front import endpoint\ndef run():\n    return endpoint()\n"}};
    ASSERT_EQ(python_proof_edges(files, 4, "lib/actual.py", "real", "Function"), 1);
    PASS();
}

TEST(resolver_python_imported_class_is_the_static_callable) {
    const python_proof_file_t files[] = {
        {"lib/errors.py", "class Problem(Exception):\n    pass\n"},
        {"lib/front.py", "from lib.errors import Problem as Error\n"},
        {"main.py", "from lib.front import Error\ndef run():\n    raise Error()\n"}};
    ASSERT_EQ(python_proof_edges(files, 3, "lib/errors.py", "Problem", "Class"), 1);
    PASS();
}

TEST(resolver_python_reexport_shadow_and_cycle_abstain) {
    const char *front[] = {
        "from lib.actual import real as endpoint\nendpoint = unknown\n",
        "from lib.actual import real as endpoint\nfrom missing import endpoint\n",
        "from lib.mid import endpoint\n"};
    for (size_t i = 0; i < sizeof(front) / sizeof(front[0]); i++) {
        const python_proof_file_t files[] = {
            {"lib/actual.py", "def real():\n    return 1\n"},
            {"lib/front.py", front[i]},
            {"lib/mid.py", "from lib.front import endpoint\n"},
            {"main.py", "from lib.front import endpoint\ndef run():\n    return endpoint()\n"}};
        ASSERT_EQ(python_proof_edges(files, 4, "lib/actual.py", "real", "Function"), 0);
    }
    PASS();
}

TEST(resolver_python_package_value_blocks_physical_submodule) {
    const python_proof_file_t files[] = {
        {"lib/__init__.py", "from .actual import real as mod\n"},
        {"lib/actual.py", "def real():\n    return 1\n"},
        {"lib/mod.py", "def wrong():\n    return 2\n"},
        {"main.py", "from lib import mod\ndef run():\n    return mod.wrong()\n"}};
    ASSERT_EQ(python_proof_edges(files, 4, "lib/mod.py", "wrong", "Function"), 0);
    PASS();
}

TEST(resolver_python_typed_receiver_preserves_crossfile_method) {
    const python_proof_file_t files[] = {
        {"lib/service.py", "class Service:\n    def go(self):\n        pass\n"},
        {"lib/decoy.py", "class Service:\n    def go(self):\n        pass\n"},
        {"main.py",
         "from lib.service import Service as Svc\ndef run(value: Svc):\n    value.go()\n"}};
    ASSERT_EQ(python_proof_edges(files, 3, "lib/service.py", "go", "Method"), 1);
    ASSERT_EQ(python_proof_edges(files, 3, "lib/decoy.py", "go", "Method"), 0);
    PASS();
}

/* ── Rules recovered on real Python, forms taken from django ──────────────
 * Each test states a positive and the decoy or shadow it must not confuse with
 * it. Every case runs both drivers: sequential, and the parallel route that the
 * filler files select. */

/* Rows of one fixture share its index, per route. A full index writes up to 57 files, and on
 * Windows every one of them is scanned, so repeating them ran this suite past the 900 s cap. */
typedef struct {
    char *key; /* every path and text of the fixture this index holds */
    char *root;
    char database[768];
    int rc;
} python_route_index_t;

static python_route_index_t python_routes[2];

static void python_route_release(python_route_index_t *route) {
    if (route->root)
        th_cleanup(route->root);
    free(route->root);
    free(route->key);
    memset(route, 0, sizeof(*route));
}

static char *python_fixture_key(const python_proof_file_t *files, size_t count) {
    size_t bytes = 1;
    for (size_t i = 0; i < count; i++)
        bytes += strlen(files[i].path) + strlen(files[i].text) + 2;
    char *key = malloc(bytes);
    if (!key)
        return NULL;
    size_t at = 0;
    for (size_t i = 0; i < count; i++)
        at += (size_t)snprintf(key + at, bytes - at, "%s\x1f%s\x1e", files[i].path, files[i].text);
    return key;
}

/* Writes and indexes the fixture for one route; the result stays until another fixture. */
static python_route_index_t *python_route_index(const python_proof_file_t *files, size_t count,
                                                bool parallel) {
    python_route_index_t *route = &python_routes[parallel ? 1 : 0];
    char *key = python_fixture_key(files, count);
    if (key && route->key && !strcmp(route->key, key)) {
        free(key);
        return route;
    }
    python_route_release(route);
    const char *made = key ? th_mktempdir("cbm_python_rule") : NULL;
    route->root = made ? strdup(made) : NULL;
    if (!route->root) {
        free(key);
        return NULL;
    }
    route->key = key;
    const char *root = route->root;
    char repo[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(route->database, sizeof(route->database), "%s/index.db", root);
    const char *database = route->database;
    bool ok = cbm_mkdir_p(repo, 0755);
    for (size_t i = 0; ok && i < count; i++) {
        char directory[1024];
        snprintf(directory, sizeof(directory), "%s/%s", repo, files[i].path);
        char *slash = strrchr(directory, '/');
        *slash = '\0';
        ok = cbm_mkdir_p(directory, 0755) &&
             th_write_file(TH_PATH(repo, files[i].path), files[i].text) == 0;
    }
    for (int i = 0; ok && parallel && i < 55; i++) {
        char path[32];
        snprintf(path, sizeof(path), "filler%02d.py", i);
        ok = th_write_file(TH_PATH(repo, path), "# unrelated source\n") == 0;
    }
    const char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    cbm_setenv("CBM_WORKERS", parallel ? "4" : "1", 1);
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "python-rule")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    route->rc = rc;
    return route;
}

static int python_route_edges(const python_proof_file_t *files, size_t count, const char *source,
                              const char *relation, const char *target, const char *name,
                              const char *label, bool parallel) {
    python_route_index_t *route = python_route_index(files, count, parallel);
    if (!route)
        return -1;
    int rc = route->rc;
    const char *database = route->database;
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (store && sqlite3_prepare_v2(cbm_store_get_db(store),
                                    "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                                    "JOIN nodes t ON t.id=e.target_id WHERE e.type=?4 "
                                    "AND s.file_path=?5 AND t.file_path=?1 AND t.name=?2 "
                                    "AND t.label=?3",
                                    -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, target, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, name, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 3, label, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 4, relation, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 5, source, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            edges = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    if (store)
        cbm_store_close(store);
    return edges;
}

/* Both drivers must agree on the expected count. */
static bool python_expect_from(const char *what, const python_proof_file_t *files, size_t count,
                               const char *source, const char *relation, const char *target,
                               const char *name, const char *label, int expected) {
    for (int parallel = 0; parallel < 2; parallel++) {
        int found = python_route_edges(files, count, source, relation, target, name, label,
                                       parallel != 0);
        if (found != expected) {
            fprintf(stderr, "python rule [%s] parallel=%d %s:%s -> %s:%s found=%d expected=%d\n",
                    what, parallel, source, relation, target, name, found, expected);
            return false;
        }
    }
    return true;
}

static bool python_expect(const char *what, const python_proof_file_t *files, size_t count,
                          const char *relation, const char *target, const char *name,
                          const char *label, int expected) {
    return python_expect_from(what, files, count, "main.py", relation, target, name, label,
                              expected);
}

#define PY_FILES(...)                                  \
    (const python_proof_file_t[]){__VA_ARGS__},        \
        sizeof((const python_proof_file_t[]){__VA_ARGS__}) / sizeof(python_proof_file_t)
#define EXPECT_CALL(what, target, name, label, expected, ...)                                     \
    ASSERT(python_expect(what, PY_FILES(__VA_ARGS__), "CALLS", target, name, label, expected))
#define EXPECT_USAGE(what, target, name, label, expected, ...)                                    \
    ASSERT(python_expect(what, PY_FILES(__VA_ARGS__), "USAGE", target, name, label, expected))

TEST(resolver_python_ordinary_syntax_keeps_the_file) {
    static const char *bodies[] = {
        "def forward(*args, **kwargs):\n    return local(*args, **kwargs)\n",
        "def label(value):\n    return f'{value!r:>4}'\ndef run():\n    return local()\n",
        "def stub(): ...\ndef run():\n    return local()\n",
        ("counter = 0\ndef bump():\n    global counter\n    counter += 1\n"
         "def run():\n    return local()\n"),
        ("def outer():\n    state = 0\n    def inner():\n        nonlocal state\n"
         "        state += 1\n    return local()\n"),
        ("def run(value):\n    match value:\n        case [first, *rest]:\n"
         "            return local()\n"),
        "note: dict[str, int]\ndef run():\n    return local()\n",
        "total = 1 + \\\n    2\ndef run():\n    return local()\n",
        "def run():\n    try:\n        return local()\n    except* ValueError:\n        pass\n"};
    for (size_t i = 0; i < sizeof(bodies) / sizeof(bodies[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source), "def local():\n    return 1\n%s", bodies[i]);
        const python_proof_file_t file = {"main.py", source};
        ASSERT(python_expect(bodies[i], &file, 1, "CALLS", "main.py", "local", "Function", 1));
    }
    /* The statement still counts for the name it rebinds. */
    EXPECT_CALL("global rebinds the name", "main.py", "local", "Function", 0,
                {"main.py", "def local():\n    return 1\ndef swap():\n    global local\n"
                            "    local = None\ndef run():\n    return local()\n"});
    EXPECT_CALL("walrus inside an interpolation binds", "main.py", "local", "Function", 0,
                {"main.py", "def local():\n    return 1\ntag = f'{(local := None)}'\n"
                            "def run():\n    return local()\n"});
    EXPECT_CALL("match capture binds", "main.py", "local", "Function", 0,
                {"main.py", "def local():\n    return 1\n"
                            "def run(value):\n    match value:\n        case local:\n"
                            "            return local()\n"});
    PASS();
}

TEST(resolver_python_only_real_namespace_effects_are_dynamic) {
    static const char *inert[] = {
        "class Node:\n    def eval(self, context):\n        return self.eval(context)\n",
        "def render(node):\n    return node.eval(1)\n",
        "def call(exec=None):\n    return exec\n",
        "def apply(target):\n    setattr(target, 'name', 1)\n    delattr(target, 'name')\n",
        "def state(obj):\n    return obj.__dict__.copy()\n",
        "def pick():\n    value = 1\n    return locals()['value']\n",
        "def attrs(obj):\n    return vars(obj)\n",
        "call(eval=1)\n",
        "def load(name):\n    return __import__(name, {}, {}, ['x'])\n",
        "def run_it(code, space):\n    exec(code, space)\n",
        "def known(code):\n    return eval(code, {}, {'x': 1})\n",
        "import sys\ndef seen():\n    return hasattr(sys.modules[__name__], 'apps')\n",
        "def where():\n    return '__file__' in globals()\n",
        "def merged():\n    return {**globals(), 'x': 1}\n",
        /* pydantic/v1/utils.py in_ipython: one literal name is only read */
        "def in_ipython():\n    try:\n        eval('__IPYTHON__')\n    except NameError:\n"
        "        return False\n    return True\n",
        "seen = eval(\"local\")\n",
        /* lib/sqlalchemy/exc.py: an alias of the namespace that is only read */
        "def translate(name):\n    glob = globals()\n    if name in glob and callable(glob[name]):\n"
        "        return glob[name]\n    return glob.get(name)\n"};
    for (size_t i = 0; i < sizeof(inert) / sizeof(inert[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "def local():\n    return 1\n%sdef run():\n    return local()\n", inert[i]);
        const python_proof_file_t file = {"main.py", source};
        ASSERT(python_expect(inert[i], &file, 1, "CALLS", "main.py", "local", "Function", 1));
    }
    static const char *effects[] = {"exec('local = None')\n", "eval('1')\n",
                                    "eval('(local := None)')\n", "eval(name)\n",
                                    "eval('local' + suffix)\n", "eval(u'local = None')\n",
                                    "glob = globals()\nglob['local'] = None\n",
                                    "__locals = locals()\n__locals['local'] = None\n",
                                    /* httpx/__init__.py: read here, but a module attribute
                                     * another file can write through */
                                    "__locals = locals()\nfor __name in []:\n"
                                    "    setattr(__locals[__name], '__module__', 'x')\n",
                                    "def f():\n    glob = globals()\n    glob.update(local=None)\n",
                                    "def f():\n    glob = globals()\n    del glob['local']\n",
                                    "def f():\n    glob = globals()\n    keep(glob)\n",
                                    "def f():\n    glob = globals()\n    return glob\n",
                                    "def f():\n    glob = globals()\n"
                                    "    for glob['local'] in [None]:\n        pass\n",
                                    "def f():\n    glob = globals()\n    a, glob['local'] = 1, None\n",
                                    "def f():\n    glob = globals()\n    glob['local'] += 1\n",
                                    "def f():\n    glob = globals()\n"
                                    "    locals()['glob']['local'] = None\n",
                                    "def f():\n    glob = globals()\n"
                                    "    def g():\n        glob['local'] = None\n    g()\n",
                                    "globals()['local'] = None\n", "locals()['local'] = None\n",
                                    "vars()['local'] = None\n",
                                    "setattr(sys.modules[__name__], 'local', None)\n"};
    for (size_t i = 0; i < sizeof(effects) / sizeof(effects[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "import sys\ndef local():\n    return 1\n%sdef run():\n    return local()\n",
                 effects[i]);
        const python_proof_file_t file = {"main.py", source};
        ASSERT(python_expect(effects[i], &file, 1, "CALLS", "main.py", "local", "Function", 0));
    }
    PASS();
}

/* `value.helper` reads value's attribute. Its spelling says nothing about the
 * module-level helper, so no usage edge may reach it. */
TEST(resolver_python_member_spelling_is_not_a_lexical_read) {
    EXPECT_USAGE("attribute read", "main.py", "helper", "Function", 0,
                 {"main.py", "def helper():\n    return 1\ndef run(value):\n    return value.helper\n"});
    EXPECT_USAGE("keyword name", "main.py", "helper", "Function", 0,
                 {"main.py", "def helper():\n    return 1\ndef run(call):\n    return call(helper=1)\n"});
    EXPECT_USAGE("plain read control", "main.py", "helper", "Function", 1,
                 {"main.py", "def helper():\n    return 1\ndef run():\n    return helper\n"});
    /* The argument carrier spells value.helper as helper over the whole attribute. */
    EXPECT_USAGE("attribute argument", "main.py", "helper", "Function", 0,
                 {"main.py", "def helper():\n    return 1\ndef run(value):\n    return check(value.helper)\n"});
    EXPECT_USAGE("attribute argument, imported spelling", "lib.py", "helper", "Function", 0,
                 {"main.py", "from lib import helper\ndef run(value):\n    return check(value.helper)\n"},
                 {"lib.py", "def helper():\n    return 1\n"});
    /* A class read through its module is a real reference, argument or not. */
    EXPECT_USAGE("module member argument", "lib.py", "Field", "Class", 1,
                 {"main.py", "import lib\ndef run(value):\n    return isinstance(value, lib.Field)\n"},
                 {"lib.py", "class Field:\n    pass\n"});
    EXPECT_USAGE("module member assigned", "lib.py", "Field", "Class", 1,
                 {"main.py", "import lib\ndef run():\n    kind = lib.Field\n    return kind\n"},
                 {"lib.py", "class Field:\n    pass\n"});
    PASS();
}

TEST(resolver_python_function_body_sees_later_module_binders) {
    EXPECT_CALL("deferred body", "main.py", "later", "Function", 1,
                {"main.py", "def run():\n    return later()\ndef later():\n    return 1\n"});
    EXPECT_CALL("module statement runs in order", "main.py", "later", "Function", 0,
                {"main.py", "value = later()\ndef later():\n    return 1\n"});
    PASS();
}

TEST(resolver_python_conditional_definitions_are_honest) {
    EXPECT_CALL("one conditional def", "main.py", "helper", "Function", 1,
                {"main.py", "import sys\nif sys.platform == 'x':\n    def helper():\n        return 1\n"
                            "def run():\n    return helper()\n"});
    EXPECT_CALL("alternative defs", "main.py", "helper", "Function", 0,
                {"main.py", "import sys\nif sys.platform == 'x':\n    def helper():\n        return 1\n"
                            "else:\n    def helper():\n        return 2\n"
                            "def run():\n    return helper()\n"});
    EXPECT_CALL("def and fallback value", "main.py", "helper", "Function", 0,
                {"main.py", "try:\n    def helper():\n        return 1\nexcept Exception:\n"
                            "    helper = None\ndef run():\n    return helper()\n"});
    PASS();
}

TEST(resolver_python_import_forms_reach_the_declaration) {
    static const python_proof_file_t util = {"pkg/util.py", "def helper():\n    return 2\n"};
    static const python_proof_file_t decoy = {"other/util.py", "def helper():\n    return 3\n"};
    static const python_proof_file_t pkg = {"pkg/__init__.py", ""};
    static const python_proof_file_t other = {"other/__init__.py", ""};
    static const char *forms[] = {
        "from pkg import util\ndef run():\n    return util.helper()\n",
        "from pkg.util import helper\ndef run():\n    return helper()\n",
        "import pkg.util\ndef run():\n    return pkg.util.helper()\n",
        "import pkg.util as u\ndef run():\n    return u.helper()\n",
        "from pkg.util import helper as h\ndef run():\n    return h()\n",
        "def run():\n    from pkg.util import helper\n    return helper()\n",
        "def run():\n    from pkg import util\n    return util.helper()\n",
        "def run():\n    import pkg.util as u\n    return u.helper()\n",
        ("import sys\nif sys.platform:\n    from pkg.util import helper\n"
         "def run():\n    return helper()\n")};
    for (size_t i = 0; i < sizeof(forms) / sizeof(forms[0]); i++) {
        const python_proof_file_t files[] = {{"main.py", forms[i]}, util, decoy, pkg, other};
        ASSERT(python_expect(forms[i], files, 5, "CALLS", "pkg/util.py", "helper", "Function", 1));
        ASSERT(python_expect(forms[i], files, 5, "CALLS", "other/util.py", "helper", "Function", 0));
    }
    /* Two dotted imports of one package bind the same root. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "import pkg.util\nimport pkg.more\nimport pkg\n"
                        "def run():\n    pkg.util.helper()\n    pkg.more.again()\n"},
            util, pkg, {"pkg/more.py", "def again():\n    return 1\n"}};
        ASSERT(python_expect("shared root", files, 4, "CALLS", "pkg/util.py", "helper", "Function", 1));
        ASSERT(python_expect("shared root", files, 4, "CALLS", "pkg/more.py", "again", "Function", 1));
    }
    /* The bound package also reads the submodules another import loaded. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "import pkg.more\nimport pkg.util as u\n"
                        "def run():\n    pkg.util.helper()\n"},
            util, pkg, {"pkg/more.py", "def again():\n    return 1\n"}};
        ASSERT(python_expect("through the root", files, 4, "CALLS", "pkg/util.py", "helper", "Function", 1));
    }
    /* A relative form, from inside the package. */
    {
        const python_proof_file_t files[] = {
            {"pkg/main.py", "from . import util\nfrom .util import helper\n"
                            "def run():\n    util.helper()\n    helper()\n"},
            util, pkg, {"main.py", "from pkg.main import run\ndef go():\n    return run()\n"}};
        ASSERT(python_expect("relative", files, 4, "CALLS", "pkg/main.py", "run", "Function", 1));
    }
    /* Two functions importing different modules under one name each see their own. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "def one():\n    from pkg.util import helper\n    return helper()\n"
                        "def two():\n    from other.util import helper\n    return helper()\n"},
            util, decoy, pkg, other};
        ASSERT(python_expect("scoped imports", files, 5, "CALLS", "pkg/util.py", "helper", "Function", 1));
        ASSERT(python_expect("scoped imports", files, 5, "CALLS", "other/util.py", "helper", "Function", 1));
    }
    /* Alternatives for one name stay ambiguous. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "try:\n    from pkg.util import helper\nexcept ImportError:\n"
                        "    from other.util import helper\ndef run():\n    return helper()\n"},
            util, decoy, pkg, other};
        ASSERT(python_expect("alternatives", files, 5, "CALLS", "pkg/util.py", "helper", "Function", 0));
        ASSERT(python_expect("alternatives", files, 5, "CALLS", "other/util.py", "helper", "Function", 0));
    }
    PASS();
}

/* django.db.__init__ does real work and django.db.models is loaded all over the
 * project: neither stops `from django.db import models` naming the submodule. */
/* Absolute imports follow sys.path as the file's own place fixes it. Ambiguity abstains. */
TEST(resolver_python_absolute_imports_follow_the_search_path) {
    static const char go[] = "def go():\n    return 1\n";
    static const char package[] = "VERSION = 1\n";
    {
        const python_proof_file_t files[] = {
            {"src/pkg/__init__.py", "from .inner import go as exported\n__all__ = ['exported']\n"},
            {"src/pkg/inner.py", go},
            {"src/other/__init__.py", package},
            {"src/other/thing.py", "def thing():\n    return 1\n"},
            {"src/pkg/user.py",
             "from pkg.inner import go\nfrom other.thing import thing\ndef run():\n    go()\n"
             "    thing()\n"}};
        ASSERT(python_expect_from("src layout, own package", files, 5, "src/pkg/user.py", "CALLS", "src/pkg/inner.py", "go", "Function", 1));
        ASSERT(python_expect_from("src layout, sibling package", files, 5, "src/pkg/user.py", "CALLS", "src/other/thing.py", "thing", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {{"src/pkg/__init__.py", ""},
                                             {"src/pkg/inner.py", go},
                                             {"src/pkg/user.py",
                                              "from pkg.inner import go\ndef run():\n    go()\n"}};
        ASSERT(python_expect_from("an empty __init__ still makes a package", files, 3, "src/pkg/user.py", "CALLS", "src/pkg/inner.py", "go", "Function", 1));
    }
    {
        /* django/tests: runtests.py runs from tests/, each test package imports by its name. */
        const python_proof_file_t files[] = {
            {"tests/admin_views/__init__.py", package},
            {"tests/admin_views/models.py", go},
            {"tests/admin_views/tests.py",
             "from admin_views.models import go\ndef run():\n    go()\n"}};
        ASSERT(python_expect_from("test package by its name", files, 3, "tests/admin_views/tests.py", "CALLS", "tests/admin_views/models.py", "go", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"scripts/helpers.py", go},
            {"scripts/run.py", "import helpers\ndef run():\n    helpers.go()\n"}};
        ASSERT(python_expect_from("script beside its module", files, 2, "scripts/run.py", "CALLS", "scripts/helpers.py", "go", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"lib/x.py", go},
            {"tests/test_a.py", "from lib.x import go\ndef run():\n    go()\n"}};
        ASSERT(python_expect_from("root module with nothing nearer", files, 2, "tests/test_a.py", "CALLS", "lib/x.py", "go", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"utils.py", go},
            {"tests/utils.py", go},
            {"tests/test_a.py", "import utils\ndef run():\n    utils.go()\n"}};
        ASSERT(python_expect_from("two candidates, root", files, 3, "tests/test_a.py", "CALLS", "utils.py", "go", "Function", 0));
        ASSERT(python_expect_from("two candidates, near", files, 3, "tests/test_a.py", "CALLS", "tests/utils.py", "go", "Function", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"utils.py", go},
            {"src/utils.py", go},
            {"src/pkg/__init__.py", package},
            {"src/pkg/user.py", "import utils\ndef run():\n    utils.go()\n"}};
        ASSERT(python_expect_from("two candidates from a package, root", files, 4, "src/pkg/user.py", "CALLS", "utils.py", "go", "Function", 0));
        ASSERT(python_expect_from("two candidates from a package, near", files, 4, "src/pkg/user.py", "CALLS", "src/utils.py", "go", "Function", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"pkg/x.py", go},
            {"src/pkg/__init__.py", package},
            {"src/pkg/x.py", go},
            {"src/pkg/user.py", "from pkg.x import go\ndef run():\n    go()\n"}};
        ASSERT(python_expect_from("own package beats a root namesake", files, 4, "src/pkg/user.py", "CALLS", "src/pkg/x.py", "go", "Function", 1));
        ASSERT(python_expect_from("the root namesake is no candidate", files, 4, "src/pkg/user.py", "CALLS", "pkg/x.py", "go", "Function", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"foo/__init__.py", package},
            {"foo/baz.py", go},
            {"tests/foo/__init__.py", package},
            {"tests/foo/bar.py", "from foo.baz import go\ndef run():\n    go()\n"}};
        ASSERT(python_expect_from("a nearer package hides the root one", files, 4, "tests/foo/bar.py", "CALLS", "foo/baz.py", "go", "Function", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"src/pkg/__init__.py", package},
            {"src/pkg/inner.py", go},
            {"src/pkg/user.py", "import inner\ndef run():\n    inner.go()\n"}};
        ASSERT(python_expect_from("no implicit relative import", files, 3, "src/pkg/user.py", "CALLS", "src/pkg/inner.py", "go", "Function", 0));
    }
    PASS();
}

TEST(resolver_python_submodule_import_survives_a_working_package) {
    static const python_proof_file_t child = {"pkg/mod.py", "def real():\n    return 1\n"};
    static const char *packages[] = {
        ("import os\nfrom pkg.other import VALUE\nDEFAULT = VALUE\nclass Handler:\n    pass\n"
         "def setup():\n    return Handler()\n"),
        "from pkg.other import *\n"};
    for (size_t i = 0; i < sizeof(packages) / sizeof(packages[0]); i++) {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", packages[i]},
            child,
            {"pkg/other.py", "__all__ = ['VALUE']\nVALUE = 1\n"},
            {"loader.py", "import pkg.mod\nimport importlib\ndef load(name):\n"
                          "    print(pkg.DEFAULT)\n"
                          "    getattr(pkg, '__file__', None)\n"
                          "    return importlib.import_module(name)\n"},
            {"main.py", "from pkg import mod\ndef run():\n    return mod.real()\n"}};
        ASSERT(python_expect(packages[i], files, 5, "CALLS", "pkg/mod.py", "real", "Function", 1));
    }
    /* A value the package binds wins over the submodule file. */
    {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "from pkg.other import *\n"},
            child,
            {"pkg/other.py", "__all__ = ['mod']\ndef mod():\n    return 1\n"},
            {"main.py", "from pkg import mod\ndef run():\n    return mod.real()\n"}};
        ASSERT(python_expect("star value", files, 4, "CALLS", "pkg/mod.py", "real", "Function", 0));
    }
    PASS();
}

TEST(resolver_python_star_import_follows_the_module_interface) {
    static const python_proof_file_t fields = {
        "pkg/fields.py", "__all__ = ['CharField']\nclass CharField:\n    pass\n"
                         "class Hidden:\n    pass\n"};
    static const python_proof_file_t plain = {
        "pkg/plain.py", "def public():\n    return 1\ndef _private():\n    return 2\n"};
    static const python_proof_file_t init = {"pkg/__init__.py",
                                             "from pkg.fields import *\nfrom pkg.plain import *\n"};
    {
        const python_proof_file_t files[] = {
            {"main.py", "from pkg import CharField, public\ndef run():\n    CharField()\n    public()\n"},
            fields, plain, init};
        ASSERT(python_expect("reexport", files, 4, "CALLS", "pkg/fields.py", "CharField", "Class", 1));
        ASSERT(python_expect("reexport", files, 4, "CALLS", "pkg/plain.py", "public", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "import pkg\ndef run():\n    pkg.CharField()\n"}, fields, plain, init};
        ASSERT(python_expect("module member", files, 4, "CALLS", "pkg/fields.py", "CharField", "Class", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "from pkg.fields import *\ndef run():\n    CharField()\n"}, fields, plain, init};
        ASSERT(python_expect("local star", files, 4, "CALLS", "pkg/fields.py", "CharField", "Class", 1));
    }
    /* Not in __all__, or private: the star does not bind it. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "from pkg import Hidden, _private\ndef run():\n    Hidden()\n    _private()\n"},
            fields, plain, init};
        ASSERT(python_expect("not exported", files, 4, "CALLS", "pkg/fields.py", "Hidden", "Class", 0));
        ASSERT(python_expect("not exported", files, 4, "CALLS", "pkg/plain.py", "_private", "Function", 0));
    }
    /* A later explicit binder wins; a star of an unknown module rebinds what precedes it. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "from pkg.fields import *\nclass CharField:\n    pass\ndef run():\n    CharField()\n"},
            fields, plain, init};
        ASSERT(python_expect("explicit after", files, 4, "CALLS", "main.py", "CharField", "Class", 1));
        ASSERT(python_expect("explicit after", files, 4, "CALLS", "pkg/fields.py", "CharField", "Class", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "from pkg.fields import CharField\nfrom external import *\n"
                        "def run():\n    CharField()\n"},
            fields, plain, init};
        ASSERT(python_expect("open star", files, 4, "CALLS", "pkg/fields.py", "CharField", "Class", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "from external import *\nfrom pkg.fields import CharField\n"
                        "def run():\n    CharField()\n"},
            fields, plain, init};
        ASSERT(python_expect("binder after open star", files, 4, "CALLS", "pkg/fields.py", "CharField", "Class", 1));
    }
    PASS();
}

TEST(resolver_python_self_selects_through_c3_order) {
    static const python_proof_file_t base = {
        "lib/base.py", "class Base:\n    def shared(self):\n        return 1\n"
                       "    def top(self):\n        return 1\n"};
    static const python_proof_file_t decoy = {
        "lib/decoy.py", "class Unrelated:\n    def shared(self):\n        return 2\n"
                        "    def only_here(self):\n        return 2\n"};
    {
        const python_proof_file_t files[] = {
            {"main.py", "from lib.base import Base\nclass Child(Base):\n"
                        "    def own(self):\n        return 1\n"
                        "    def run(self):\n        self.own()\n        self.shared()\n"
                        "        self.only_here()\n"},
            base, decoy};
        ASSERT(python_expect("own", files, 3, "CALLS", "main.py", "own", "Method", 1));
        ASSERT(python_expect("inherited", files, 3, "CALLS", "lib/base.py", "shared", "Method", 1));
        ASSERT(python_expect("decoy", files, 3, "CALLS", "lib/decoy.py", "shared", "Method", 0));
        ASSERT(python_expect("unique name elsewhere", files, 3, "CALLS", "lib/decoy.py", "only_here", "Method", 0));
    }
    /* Diamond: C3 puts Right before Top, where depth-first would find Top first. */
    {
        const python_proof_file_t files[] = {
            {"main.py",
             "class Top:\n    def pick(self):\n        return 0\n"
             "class Left(Top):\n    pass\n"
             "class Right(Top):\n    def pick(self):\n        return 1\n"
             "class Bottom(Left, Right):\n    def run(self):\n        return self.pick()\n"}};
        ASSERT(python_expect("diamond", files, 1, "CALLS", "main.py", "pick", "Method", 1));
    }
    {
        const char *source =
            "class Top:\n    def pick(self):\n        return 0\n"
            "class Left(Top):\n    pass\n"
            "class Right(Top):\n    def pick(self):\n        return 1\n"
            "class Bottom(Left, Right):\n    def run(self):\n        return self.pick()\n";
        const char *root = th_mktempdir("cbm_python_c3");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        ASSERT(cbm_mkdir_p(repo, 0755));
        ASSERT_EQ(th_write_file(TH_PATH(repo, "main.py"), source), 0);
        cbm_pipeline_t *pipeline = cbm_pipeline_new(repo, database, CBM_MODE_FULL);
        ASSERT_EQ(cbm_pipeline_set_project_name(pipeline, "python-c3") ? cbm_pipeline_run(pipeline) : -1, 0);
        cbm_pipeline_free(pipeline);
        cbm_store_t *store = cbm_store_open_path_existing(database);
        ASSERT_NOT_NULL(store);
        sqlite3_stmt *statement = NULL;
        ASSERT_EQ(sqlite3_prepare_v2(cbm_store_get_db(store),
                                     "SELECT t.qualified_name FROM edges e JOIN nodes s ON s.id=e.source_id "
                                     "JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND s.name='run'",
                                     -1, &statement, NULL),
                  SQLITE_OK);
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        ASSERT_NOT_NULL(strstr((const char *)sqlite3_column_text(statement, 0), ".Right.pick"));
        ASSERT_EQ(sqlite3_step(statement), SQLITE_DONE);
        sqlite3_finalize(statement);
        cbm_store_close(store);
        th_cleanup(root);
    }
    /* The proven base is a real INHERITS edge, through a reexport or a star. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "import pkg\nfrom pkg import Base as Named\n"
                        "class Dotted(pkg.Base):\n    pass\nclass Aliased(Named):\n    pass\n"},
            {"pkg/__init__.py", "from pkg.impl import *\n"},
            {"pkg/impl.py", "__all__ = ['Base']\nclass Base:\n    pass\nclass Decoy:\n    pass\n"}};
        ASSERT(python_expect("reexported base", files, 3, "INHERITS", "pkg/impl.py", "Base", "Class", 2));
        ASSERT(python_expect("reexported base decoy", files, 3, "INHERITS", "pkg/impl.py", "Decoy", "Class", 0));
    }
    /* What is not certain stops the walk, and own members never depend on it. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "from lib.base import Base\nfrom external import Ext\n"
                        "class First(Ext, Base):\n"
                        "    def own(self):\n        return 1\n"
                        "    def run(self):\n        self.own()\n        self.top()\n"},
            base};
        ASSERT(python_expect("own before external", files, 2, "CALLS", "main.py", "own", "Method", 1));
        ASSERT(python_expect("external first", files, 2, "CALLS", "lib/base.py", "top", "Method", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "from lib.base import Base\nfrom external import Ext\n"
                        "class Last(Base, Ext):\n    def run(self):\n        self.top()\n"},
            base};
        ASSERT(python_expect("external last", files, 2, "CALLS", "lib/base.py", "top", "Method", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "from lib.base import Base\nclass Made(make_base(), Base):\n"
                        "    def own(self):\n        return 1\n"
                        "    def run(self):\n        self.own()\n        self.top()\n"},
            base};
        ASSERT(python_expect("computed base, own", files, 2, "CALLS", "main.py", "own", "Method", 1));
        ASSERT(python_expect("computed base", files, 2, "CALLS", "lib/base.py", "top", "Method", 0));
    }
    /* An unknown base further up can sit ahead of a sibling in the real order. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "from lib.base import Base\n"
                        "class Middle(make_base()):\n    def mid(self):\n        return 1\n"
                        "class Multi(Middle, Base):\n    def run(self):\n"
                        "        self.mid()\n        self.top()\n"
                        "class Single(Middle):\n    def run(self):\n        return self.mid()\n"},
            base};
        ASSERT(python_expect("sibling after unknown ancestry", files, 2, "CALLS", "lib/base.py", "top", "Method", 0));
        /* Single.run through its only base, Multi.run through its first written base. */
        ASSERT(python_expect("single chain and first base keep their order", files, 2, "CALLS", "main.py", "mid", "Method", 2));
    }
    /* C3 puts the first written base directly behind its class, or refuses the class. So
     * an unknown second base cannot overtake it, from django TupleIn(TupleLookupMixin, In).
     * Nothing behind that base is certain: the unknown one can subclass its parent. */
    {
        const char *classes =
            "class Root:\n    def far(self):\n        return 1\n"
            "class Mixin(Root):\n    def near(self):\n        return 1\n"
            "    def both(self):\n        return 1\n";
        static const char *const first_cases[][2] = {
            {"first base before an unknown one",
             "class A(Mixin, made()):\n    def run(self):\n        return self.near()\n"},
            {"first base through a subclass",
             "class A(Mixin, made()):\n    pass\nclass B(A):\n    def run(self):\n"
             "        return self.near()\n"},
            {"own member wins over the first base",
             "class A(Mixin, made()):\n    def near(self):\n        return 2\n"
             "    def run(self):\n        return self.near()\n"},
        };
        static const int first_expected[] = {1, 1, 0};
        for (size_t i = 0; i < 3; i++) {
            char source[1024];
            snprintf(source, sizeof(source), "%s%s", classes, first_cases[i][1]);
            const python_proof_file_t files[] = {{"main.py", source}, base};
            /* A count by name in one file: the own near is a second declaration of that name. */
            int expected = first_expected[i] + (i == 2 ? 1 : 0);
            ASSERT(python_expect(first_cases[i][0], files, 2, "CALLS", "main.py", "near", "Method", expected));
        }
        char source[1024];
        snprintf(source, sizeof(source), "%s%s", classes,
                 "class A(Mixin, made()):\n    def run(self):\n        return self.far()\n"
                 "class C(made(), Mixin):\n    def run(self):\n        return self.both()\n");
        const python_proof_file_t files[] = {{"main.py", source}, base};
        ASSERT(python_expect("behind the first base nothing is certain", files, 2, "CALLS", "main.py", "far", "Method", 0));
        ASSERT(python_expect("a mixin behind an unknown first base", files, 2, "CALLS", "main.py", "both", "Method", 0));
    }
    /* Base[int] inherits Base, unless the class answers the subscript itself. */
    {
        const char *generic =
            "from typing import Generic, TypeVar\nT = TypeVar('T')\n"
            "class Repo(Generic[T]):\n    def load(self):\n        return 1\n"
            "class UserRepo(Repo[int]):\n    def run(self):\n        return self.load()\n";
        const python_proof_file_t files[] = {{"main.py", generic}};
        ASSERT(python_expect("subscripted base", files, 1, "CALLS", "main.py", "load", "Method", 1));
        const char *answers =
            "class Other:\n    def load(self):\n        return 2\n"
            "class Weird:\n    def __class_getitem__(cls, item):\n        return Other\n"
            "    def load(self):\n        return 1\n"
            "class Sub(Weird):\n    pass\n";
        char source[512];
        snprintf(source, sizeof(source), "%s%s", answers,
                 "class A(Weird[int]):\n    def run(self):\n        return self.load()\n"
                 "class B(Sub[int]):\n    def run(self):\n        return self.load()\n");
        const python_proof_file_t subscript[] = {{"main.py", source}};
        ASSERT(python_expect("a class that answers its subscript", subscript, 1, "CALLS", "main.py", "load", "Method", 0));
        snprintf(source, sizeof(source), "%s%s", answers,
                 "class A(Weird):\n    def run(self):\n        return self.load()\n");
        const python_proof_file_t plain[] = {{"main.py", source}};
        ASSERT(python_expect("the same class, not subscripted", plain, 1, "CALLS", "main.py", "load", "Method", 1));
    }
    /* The instance and the class can answer differently. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "class Stored:\n    def go(self):\n        return 1\n"
                        "    def __init__(self):\n        self.go = other\n"
                        "    def run(self):\n        return self.go()\n"}};
        ASSERT(python_expect("instance rebinding", files, 1, "CALLS", "main.py", "go", "Method", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "class Hooked:\n    def go(self):\n        return 1\n"
                        "    def __getattribute__(self, name):\n        return other\n"
                        "    def run(self):\n        return self.go()\n"}};
        ASSERT(python_expect("getattribute", files, 1, "CALLS", "main.py", "go", "Method", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "class Parent:\n    def go(self):\n        return 1\n"
                        "class Value(Parent):\n    go = make_go()\n"
                        "    def run(self):\n        return self.go()\n"}};
        ASSERT(python_expect("class value stops lookup", files, 1, "CALLS", "main.py", "go", "Method", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "class Rebound:\n    def go(self):\n        return 1\n"
                        "    def run(self):\n        self = other\n        return self.go()\n"}};
        ASSERT(python_expect("self reassigned", files, 1, "CALLS", "main.py", "go", "Method", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py", "class Parent:\n    def __hidden(self):\n        return 1\n"
                        "class Sub(Parent):\n    def run(self):\n        return self.__hidden()\n"}};
        ASSERT(python_expect("name mangling", files, 1, "CALLS", "main.py", "__hidden", "Method", 0));
    }
    PASS();
}

TEST(resolver_python_super_and_class_level_receivers) {
    static const python_proof_file_t base = {
        "lib/base.py", "class Base:\n    def __init__(self):\n        pass\n"
                       "    def save(self):\n        return 1\n"
                       "    @classmethod\n    def build(cls):\n        return cls()\n"
                       "    @staticmethod\n    def plain():\n        return 1\n"};
    {
        const python_proof_file_t files[] = {
            {"main.py", "from lib.base import Base\nclass Child(Base):\n"
                        "    def __init__(self):\n        super().__init__()\n"
                        "    def save(self):\n        return super().save()\n"
                        "    def old(self):\n        return super(Child, self).save()\n"
                        "    @classmethod\n    def make(cls):\n        return cls.build()\n"
                        "    def fixed(self):\n        return self.plain()\n"
                        "def direct():\n    Base.build()\n    return Base.plain()\n"},
            base};
        ASSERT(python_expect("super init", files, 2, "CALLS", "lib/base.py", "__init__", "Method", 1));
        ASSERT(python_expect("super method", files, 2, "CALLS", "lib/base.py", "save", "Method", 2));
        ASSERT(python_expect("cls and class", files, 2, "CALLS", "lib/base.py", "build", "Method", 2));
        ASSERT(python_expect("static", files, 2, "CALLS", "lib/base.py", "plain", "Method", 2));
    }
    /* super() skips the writing class. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "class Only:\n    def save(self):\n        return super().save()\n"}};
        ASSERT(python_expect("no base method", files, 1, "CALLS", "main.py", "save", "Method", 0));
    }
    /* staticmethod's first parameter is an ordinary argument. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "class Tool:\n    def go(self):\n        return 1\n"
                        "    @staticmethod\n    def run(self):\n        return self.go()\n"}};
        ASSERT(python_expect("static parameter", files, 1, "CALLS", "main.py", "go", "Method", 0));
    }
    PASS();
}

/* The class of a value comes from a fact about it. self.compiler below has none,
 * however unique apply_converters is: that stays an abstention. */
TEST(resolver_python_receiver_types_need_a_fact) {
    static const python_proof_file_t store = {
        "lib/store.py", "class Store:\n    def get(self):\n        return 1\n"
                        "    def apply_converters(self):\n        return 1\n"
                        "class Other:\n    def get(self):\n        return 2\n"
                        "def make() -> Store:\n    return Store()\n"
                        "async def amake() -> Store:\n    return Store()\n"
                        "def untyped():\n    return Store()\n"
                        "def mixed(flag):\n    if flag:\n        return Store()\n"
                        "    return Other()\n"
                        "def echo(value):\n    return value\n"
                        "def stream():\n    yield Store()\n"
                        "def again():\n    return again()\n"
                        "class Fluent(Store):\n"
                        "    def chain(self):\n        return self\n"
                        "    def clone(self):\n        made = self.__class__()\n        return made\n"};
    static const struct {
        const char *what, *source;
        int store_get;
    } cases[] = {
        {"attribute from construction",
         "from lib.store import Store\nclass A:\n    def __init__(self):\n        self.s = Store()\n"
         "    def run(self):\n        return self.s.get()\n",
         1},
        {"attribute with a None default",
         "from lib.store import Store\nclass A:\n    def __init__(self):\n        self.s = None\n"
         "    def load(self):\n        self.s = Store()\n"
         "    def run(self):\n        return self.s.get()\n",
         1},
        {"attribute with a None class default",
         "from lib.store import Store\nclass A:\n    s = None\n"
         "    def __init__(self):\n        self.s = Store()\n"
         "    def run(self):\n        return self.s.get()\n",
         1},
        {"attribute from annotated parameter",
         "from lib.store import Store\nclass A:\n    def __init__(self, s: Store):\n        self.s = s\n"
         "    def run(self):\n        return self.s.get()\n",
         1},
        {"attribute annotation",
         "from lib.store import Store\nclass A:\n    s: Store\n"
         "    def run(self):\n        return self.s.get()\n",
         1},
        {"attribute inherited from base",
         "from lib.store import Store\nclass A:\n    def __init__(self):\n        self.s = Store()\n"
         "class B(A):\n    def run(self):\n        return self.s.get()\n",
         1},
        {"attribute with two types",
         "from lib.store import Store, Other\nclass A:\n    def one(self):\n        self.s = Store()\n"
         "    def two(self):\n        self.s = Other()\n"
         "    def run(self):\n        return self.s.get()\n",
         0},
        {"attribute from an untyped parameter",
         "class A:\n    def __init__(self, s):\n        self.s = s\n"
         "    def run(self):\n        return self.s.get()\n",
         0},
        {"local from construction",
         "from lib.store import Store\ndef run():\n    s = Store()\n    return s.get()\n", 1},
        {"local copied",
         "from lib.store import Store\ndef run():\n    s = Store()\n    t = s\n    return t.get()\n", 1},
        {"local reassigned",
         "from lib.store import Store\ndef run(flag):\n    s = Store()\n    s = flag\n    return s.get()\n",
         0},
        {"local with a None start",
         "from lib.store import Store\ndef run(flag):\n    s = None\n    if flag:\n"
         "        s = Store()\n    return s.get()\n",
         1},
        {"local reassigned to the same class",
         "from lib.store import Store\ndef run(flag):\n    s = Store()\n    if flag:\n"
         "        s = Store()\n    return s.get()\n",
         1},
        {"local rebound through a fluent method",
         "from lib.store import Fluent\ndef run():\n    s = Fluent()\n    s = s.chain()\n"
         "    return s.get()\n",
         1},
        {"local rebound through an unknown step",
         "from lib.store import Fluent\ndef run():\n    s = Fluent()\n    s = s.other()\n"
         "    return s.get()\n",
         0},
        {"parameter annotation",
         "from lib.store import Store\ndef run(s: Store):\n    return s.get()\n", 1},
        {"optional annotation",
         "from typing import Optional\nfrom lib.store import Store\n"
         "def run(s: Optional[Store]):\n    return s.get()\n",
         1},
        {"qualified optional annotation",
         "import typing\nfrom lib.store import Store\ndef run(s: typing.Optional[Store]):\n"
         "    return s.get()\n",
         1},
        {"aliased optional annotation",
         "from typing import Optional as Maybe\nfrom lib.store import Store\n"
         "def run(s: Maybe[Store]):\n    return s.get()\n",
         1},
        {"a project class named Optional says nothing",
         "from lib.maybe import Optional\nfrom lib.store import Store\n"
         "def run(s: Optional[Store]):\n    return s.get()\n",
         0},
        {"optional that was never imported",
         "from lib.store import Store\ndef run(s: Optional[Store]):\n    return s.get()\n", 0},
        {"union with None on the right",
         "from lib.store import Store\ndef run(s: Store | None):\n    return s.get()\n", 1},
        {"union with None on the left",
         "from lib.store import Store\ndef run(s: None | Store):\n    return s.get()\n", 1},
        {"union of two classes",
         "from lib.store import Store, Other\ndef run(s: Store | Other):\n    return s.get()\n", 0},
        {"union of two classes and None",
         "from lib.store import Store, Other\ndef run(s: Store | Other | None):\n    return s.get()\n", 0},
        {"optional of a union",
         "from typing import Optional\nfrom lib.store import Store, Other\n"
         "def run(s: Optional[Store | Other]):\n    return s.get()\n",
         0},
        {"attribute from a union parameter",
         "from lib.store import Store\nclass A:\n    def __init__(self, s: Store | None):\n"
         "        self.s = s\n    def run(self):\n        return self.s.get()\n",
         1},
        {"forward reference annotation",
         "from lib.store import Store\ndef run(s: 'Store'):\n    return s.get()\n", 1},
        {"annotated return",
         "from lib.store import make\ndef run():\n    return make().get()\n", 1},
        {"return inferred from its construction",
         "from lib.store import untyped\ndef run():\n    return untyped().get()\n", 1},
        {"returns disagree",
         "from lib.store import mixed\ndef run():\n    return mixed(1).get()\n", 0},
        {"returns a parameter",
         "from lib.store import echo\ndef run(x):\n    return echo(x).get()\n", 0},
        {"generator",
         "from lib.store import stream\ndef run():\n    return stream().get()\n", 0},
        {"recursion proves nothing",
         "from lib.store import again\ndef run():\n    return again().get()\n", 0},
        {"fluent method returning self",
         "from lib.store import Fluent\ndef run():\n    return Fluent().chain().get()\n", 1},
        {"method returning a clone of its class",
         "from lib.store import Fluent\ndef run():\n    return Fluent().clone().get()\n", 1},
        {"direct construction", "from lib.store import Store\ndef run():\n    return Store().get()\n", 1},
        {"awaited annotated coroutine",
         "from lib.store import amake\nasync def run():\n    s = await amake()\n    return s.get()\n",
         1},
        {"untyped parameter", "def run(s):\n    return s.get()\n", 0},
        {"parameter shadows the class",
         "from lib.store import Store\ndef run(Store):\n    return Store().get()\n", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const python_proof_file_t files[] = {
            {"main.py", cases[i].source},
            store,
            {"lib/maybe.py", "class Optional:\n    def __class_getitem__(cls, item):\n"
                             "        return cls\n"}};
        ASSERT(python_expect(cases[i].what, files, 3, "CALLS", "lib/store.py", "get", "Method",
                             cases[i].store_get));
    }
    /* The last assignment selects, and the earlier class is no candidate. A branch
     * leaves both possible, so neither is selected. */
    {
        const python_proof_file_t elsewhere = {
            "lib/other.py", "class Elsewhere:\n    def get(self):\n        return 2\n"};
        const python_proof_file_t rebound[] = {
            {"main.py", "from lib.store import Store\nfrom lib.other import Elsewhere\n"
                        "def run():\n    s = Store()\n    s = Elsewhere()\n    return s.get()\n"},
            store,
            elsewhere};
        ASSERT(python_expect("rebound leaves the first class", rebound, 3, "CALLS", "lib/store.py",
                             "get", "Method", 0));
        ASSERT(python_expect("rebound reaches the last class", rebound, 3, "CALLS", "lib/other.py",
                             "get", "Method", 1));
        const python_proof_file_t branch[] = {
            {"main.py", "from lib.store import Store\nfrom lib.other import Elsewhere\n"
                        "def run(flag):\n    s = Store()\n    if flag:\n"
                        "        s = Elsewhere()\n    return s.get()\n"},
            store,
            elsewhere};
        ASSERT(python_expect("branch selects neither", branch, 3, "CALLS", "lib/store.py", "get",
                             "Method", 0));
        ASSERT(python_expect("branch selects neither", branch, 3, "CALLS", "lib/other.py", "get",
                             "Method", 0));
    }
    /* TYPE_CHECKING imports name classes for annotations and bind nothing at runtime. */
    EXPECT_CALL("annotation through TYPE_CHECKING", "lib/store.py", "get", "Method", 1,
                {"main.py", "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
                            "    from lib.store import Store\n"
                            "def run(s: 'Store'):\n    return s.get()\n"},
                store);
    EXPECT_CALL("no runtime binding through TYPE_CHECKING", "lib/store.py", "Store", "Class", 0,
                {"main.py", "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
                            "    from lib.store import Store\n"
                            "def run():\n    return Store()\n"},
                store);
    EXPECT_CALL("custom __new__ can return anything", "lib/made.py", "go", "Method", 0,
                {"main.py", "from lib.made import Made\ndef run():\n    return Made().go()\n"},
                {"lib/made.py", "class Made:\n    def __new__(cls):\n        return other\n"
                                "    def go(self):\n        return 1\n"});
    EXPECT_CALL("untyped attribute keeps abstaining", "lib/store.py", "apply_converters", "Method", 0,
                {"main.py", "class A:\n    def __init__(self, compiler):\n        self.compiler = compiler\n"
                            "    def run(self):\n        return self.compiler.apply_converters()\n"},
                store);
    PASS();
}

/* A receiver fact holds only while the name still means that binding. Every form that
 * rebinds it, and every `self` that is not the method's own, abstains. */
/* A class body's assignment says what a read yields only while nothing can replace it. In
 * django `self.title` on a model is a str, never the CharField the body assigned. */
/* A class body's value is the attribute's declared value. django's objects = UserManager() reads
 * as what it says under ModelBase; only the standard library's Enum and NamedTuple state that a
 * member reads as something else. */
TEST(resolver_python_class_body_values_can_be_replaced) {
    static const struct {
        const char *what;
        const char *body;
        int expected;
    } cases[] = {
        {"plain class attribute",
         "class A:\n    title = Field()\n    def run(self):\n        return self.title.clean()\n", 1},
        {"inherited plain class attribute",
         "class A:\n    title = Field()\nclass B(A):\n    def run(self):\n"
         "        return self.title.clean()\n",
         1},
        {"own metaclass",
         "class A(metaclass=Meta):\n    title = Field()\n    def run(self):\n"
         "        return self.title.clean()\n",
         1},
        {"metaclass on an ancestor",
         "class M(metaclass=Meta):\n    pass\nclass A(M):\n    title = Field()\n"
         "    def run(self):\n        return self.title.clean()\n",
         1},
        {"metaclass on the receiver's own class",
         "class P:\n    title = Field()\nclass A(P, metaclass=Meta):\n    def run(self):\n"
         "        return self.title.clean()\n",
         1},
        {"read from outside, metaclass",
         "class A(metaclass=Meta):\n    title = Field()\ndef run():\n    return A.title.clean()\n", 1},
        {"__init_subclass__",
         "class H:\n    def __init_subclass__(cls):\n        pass\nclass A(H):\n"
         "    title = Field()\n    def run(self):\n        return self.title.clean()\n",
         1},
        {"enum member",
         "from enum import Enum\nclass A(Enum):\n    RED = Field()\n    def run(self):\n"
         "        return self.RED.clean()\n",
         0},
        {"enum member from outside",
         "from enum import Enum\nclass A(Enum):\n    RED = Field()\ndef run():\n"
         "    return A.RED.clean()\n",
         0},
        /* From django's tests: rf = RequestFactory() in a unittest.TestCase is what it says. */
        {"external base keeps it",
         "from unittest import TestCase\nclass A(TestCase):\n    title = Field()\n"
         "    def run(self):\n        return self.title.clean()\n",
         1},
        {"aliased enum",
         "import enum as e\nclass A(e.IntEnum):\n    RED = Field()\n    def run(self):\n"
         "        return self.RED.clean()\n",
         0},
        {"named tuple field",
         "from typing import NamedTuple\nclass A(NamedTuple):\n    title = Field()\n"
         "    def run(self):\n        return self.title.clean()\n",
         0},
        {"a project class named Enum",
         "from lib.enum import Enum\nclass A(Enum):\n    RED = Field()\n    def run(self):\n"
         "        return self.RED.clean()\n",
         1},
        {"unknown base",
         "class A(make_base()):\n    title = Field()\n    def run(self):\n"
         "        return self.title.clean()\n",
         0},
        {"instance store under a metaclass",
         "class A(metaclass=Meta):\n    def __init__(self):\n        self.title = Field()\n"
         "    def run(self):\n        return self.title.clean()\n",
         1},
        {"annotation under a metaclass",
         "class A(metaclass=Meta):\n    title: Field\n    def run(self):\n"
         "        return self.title.clean()\n",
         1},
        {"a class stored as a value under a metaclass",
         "class A(metaclass=Meta):\n    kind = Field\n    def run(self):\n"
         "        return self.kind().clean()\n",
         1},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[1024];
        snprintf(source, sizeof(source),
                 "class Meta(type):\n    pass\nclass Field:\n    def clean(self):\n"
                 "        return 1\n%s",
                 cases[i].body);
        const python_proof_file_t files[] = {{"main.py", source},
                                             {"lib/enum.py", "class Enum:\n    pass\n"}};
        ASSERT(python_expect(cases[i].what, files, 2, "CALLS", "main.py", "clean", "Method", cases[i].expected));
    }
    PASS();
}

/* A comprehension body does not see the class it is written in. */
TEST(resolver_python_class_comprehension_skips_the_class) {
    const char *shadowed = "def helper():\n    return 1\nclass A:\n    def helper(self):\n"
                           "        return 2\n    values = [helper() for _ in ()]\n";
    const python_proof_file_t both[] = {{"main.py", shadowed}};
    ASSERT(python_expect("never the class's name", both, 1, "CALLS", "main.py", "helper", "Method", 0));
    /* KNOWN GAP, not a contract: the module function is what runs. A comprehension has no
     * scope of its own in the index, so a name the class binds abstains. */
    ASSERT(python_expect("the module function abstains", both, 1, "CALLS", "main.py", "helper", "Function", 0));
    const char *outer = "def helper():\n    return 1\nclass A:\n"
                        "    values = [helper() for _ in ()]\n";
    const python_proof_file_t only[] = {{"main.py", outer}};
    ASSERT(python_expect("a name the class does not bind", only, 1, "CALLS", "main.py", "helper", "Function", 1));
    const char *direct = "class A:\n    def helper(value=None):\n        return 2\n"
                         "    value = helper()\n";
    const python_proof_file_t body[] = {{"main.py", direct}};
    ASSERT(python_expect("the class body itself does see it", body, 1, "CALLS", "main.py", "helper", "Method", 1));
    PASS();
}

TEST(resolver_python_shadowed_super_and_type_are_not_the_builtins) {
    static const char base[] = "class Base:\n    def go(self):\n        return 1\n";
    static const struct {
        const char *what;
        const char *body;
        int expected;
    } cases[] = {
        {"builtin super", "class A(Base):\n    def run(self):\n        return super().go()\n", 1},
        {"builtin type", "class A(Base):\n    def run(self):\n        return type(self).go(self)\n", 1},
        {"module function named super",
         "def super():\n    return 1\nclass A(Base):\n    def run(self):\n        return super().go()\n",
         0},
        {"module function named type",
         "def type(value):\n    return 1\nclass A(Base):\n    def run(self):\n"
         "        return type(self).go(self)\n",
         0},
        {"import named super",
         "from lib.names import super\nclass A(Base):\n    def run(self):\n        return super().go()\n",
         0},
        {"local named super",
         "class A(Base):\n    def run(self, super):\n        return super().go()\n", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[1024];
        snprintf(source, sizeof(source), "%s%s", base, cases[i].body);
        const python_proof_file_t files[] = {
            {"main.py", source}, {"lib/names.py", "def super():\n    return 1\n"}};
        ASSERT(python_expect(cases[i].what, files, 2, "CALLS", "main.py", "go", "Method", cases[i].expected));
    }
    PASS();
}

TEST(resolver_python_rebound_receivers_abstain) {
    static const struct {
        const char *what;
        const char *body;
        int expected;
    } cases[] = {
        {"control, one construction", "def run():\n    s = Store()\n    return s.get()\n", 1},
        {"construction in one arm", "def run(flag):\n    if flag:\n        s = Store()\n    return s.get()\n", 1},
        {"for binder", "def run(items):\n    s = Store()\n    for s in items:\n        s.get()\n", 0},
        {"with binder", "def run(thing):\n    s = Store()\n    with thing as s:\n        return s.get()\n", 0},
        {"except binder",
         "def run():\n    s = Store()\n    try:\n        return 1\n    except ValueError as s:\n"
         "        return s.get()\n",
         0},
        {"walrus", "def run(other):\n    s = Store()\n    if (s := other):\n        return s.get()\n", 0},
        {"import", "def run():\n    s = Store()\n    import os as s\n    return s.get()\n", 0},
        {"nested def", "def run():\n    s = Store()\n    def s():\n        return 1\n    return s.get()\n", 0},
        {"nonlocal write",
         "def run(other):\n    s = Store()\n    def change():\n        nonlocal s\n        s = other\n"
         "    change()\n    return s.get()\n",
         0},
        {"augmented", "def run(other):\n    s = Store()\n    s += other\n    return s.get()\n", 0},
        {"tuple target", "def run(pair):\n    s = Store()\n    s, t = pair\n    return s.get()\n", 0},
        {"star target", "def run(items):\n    s = Store()\n    *s, t = items\n    return s.get()\n", 0},
        {"deleted", "def run():\n    s = Store()\n    del s\n    return s.get()\n", 0},
        /* What the signature says it holds unless told otherwise, as a checker reads it. */
        {"parameter default", "def run(s=Store()):\n    return s.get()\n", 1},
        {"annotated then reassigned", "def run(other, s: Store):\n    s = other\n    return s.get()\n", 0},
        {"self rebound", "class A(Store):\n    def run(self, other):\n        self = other\n        return self.get()\n", 0},
        {"static parameter named self", "class A(Store):\n    @staticmethod\n    def run(self):\n        return self.get()\n", 0},
        {"nested parameter named self",
         "class A(Store):\n    def run(self):\n        def inner(self):\n            return self.get()\n"
         "        return inner\n",
         0},
        {"lambda parameter named self", "class A(Store):\n    def run(self):\n        return lambda self: self.get()\n", 0},
        {"module function parameter named self", "def run(self):\n    return self.get()\n", 0},
        {"closure keeps the method receiver",
         "class A(Store):\n    def run(self):\n        def inner():\n            return self.get()\n"
         "        return inner\n",
         1},
        {"__getattribute__ answers every read",
         "class A(Store):\n    def __getattribute__(self, name):\n        return name\n"
         "    def run(self):\n        return self.get()\n",
         0},
        {"__getattr__ answers only what is missing",
         "class A(Store):\n    def __getattr__(self, name):\n        return name\n"
         "    def run(self):\n        return self.get()\n",
         1},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[1024];
        snprintf(source, sizeof(source), "class Store:\n    def get(self):\n        return 1\n%s",
                 cases[i].body);
        const python_proof_file_t files[] = {{"main.py", source}};
        ASSERT(python_expect(cases[i].what, files, 1, "CALLS", "main.py", "get", "Method", cases[i].expected));
    }
    /* Either arm can bind the class, so neither is selected. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "try:\n    from lib.a import S\nexcept ImportError:\n    from lib.b import S\n"
                        "def run():\n    return S().shared()\n"},
            {"lib/__init__.py", ""},
            {"lib/a.py", "class S:\n    def shared(self):\n        return 1\n"},
            {"lib/b.py", "class S:\n    def shared(self):\n        return 2\n"}};
        ASSERT(python_expect("import in both arms", files, 4, "CALLS", "lib/a.py", "shared", "Method", 0));
        ASSERT(python_expect("import in both arms", files, 4, "CALLS", "lib/b.py", "shared", "Method", 0));
    }
    PASS();
}

TEST(resolver_python_module_level_instance_has_its_class) {
    static const python_proof_file_t router = {
        "pkg/__init__.py", "from pkg.routing import Router\nrouter = Router()\n"};
    static const python_proof_file_t definition = {
        "pkg/routing.py", "class Router:\n    def route(self):\n        return 1\n"};
    EXPECT_CALL("imported instance", "pkg/routing.py", "route", "Method", 1,
                {"main.py", "from pkg import router\ndef run():\n    return router.route()\n"},
                router, definition);
    EXPECT_CALL("rebound instance", "pkg/routing.py", "route", "Method", 0,
                {"main.py", "from pkg import router\ndef run():\n    return router.route()\n"},
                {"pkg/__init__.py", "from pkg.routing import Router\nrouter = Router()\nrouter = None\n"},
                definition);
    /* Calling a module variable calls what it holds, when that has a fact. */
    EXPECT_CALL("variable holds a bound method", "pkg/routing.py", "route", "Method", 1,
                {"main.py", "from pkg import send\ndef run():\n    return send()\n"},
                {"pkg/__init__.py", "from pkg.routing import Router\nrouter = Router()\n"
                                    "send = router.route\n"},
                definition);
    EXPECT_CALL("instance call runs __call__", "lib.py", "__call__", "Method", 1,
                {"main.py", "from lib import handler\ndef run():\n    return handler()\n"},
                {"lib.py", "class Handler:\n    def __call__(self):\n        return 1\n"
                           "handler = Handler()\n"});
    EXPECT_CALL("variable holds an unknown value", "lib.py", "real", "Function", 0,
                {"main.py", "from lib import send\ndef run():\n    return send()\n"},
                {"lib.py", "def real():\n    return 1\nsend = make(real)\n"});
    EXPECT_CALL("local alias of a function", "lib.py", "real", "Function", 1,
                {"main.py", "from lib import real\ndef run():\n    call = real\n    return call()\n"},
                {"lib.py", "def real():\n    return 1\n"});
    EXPECT_CALL("functools.partial calls what it wraps", "lib.py", "real", "Function", 1,
                {"main.py", "from lib import send\ndef run():\n    return send()\n"},
                {"lib.py", "from functools import partial\ndef real(a):\n    return a\n"
                           "send = partial(real, 1)\n"});
    EXPECT_CALL("functools.partial spelled through the module", "lib.py", "real", "Function", 1,
                {"main.py", "from lib import send\ndef run():\n    return send()\n"},
                {"lib.py", "import functools\ndef real(a):\n    return a\n"
                           "send = functools.partial(real, 1)\n"});
    EXPECT_CALL("a project partial is not functools.partial", "lib.py", "real", "Function", 0,
                {"main.py", "from lib import send\ndef run():\n    return send()\n"},
                {"lib.py", "from mine import partial\ndef real(a):\n    return a\n"
                           "send = partial(real, 1)\n"});
    EXPECT_USAGE("module constant", "lib.py", "LIMIT", "Variable", 1,
                 {"main.py", "from lib import LIMIT\ndef run():\n    return LIMIT\n"},
                 {"lib.py", "LIMIT = 3\n"});
    EXPECT_USAGE("module constant rebound", "lib.py", "LIMIT", "Variable", 0,
                 {"main.py", "from lib import LIMIT\ndef run():\n    return LIMIT\n"},
                 {"lib.py", "LIMIT = 3\nLIMIT = 4\n"});
    PASS();
}

TEST(resolver_python_decorators_keep_identity_only_when_proven) {
    static const python_proof_file_t decorators = {
        "lib/deco.py",
        "from functools import wraps, update_wrapper\n"
        "def wraps_called(function):\n"
        "    def inner(*args):\n        return function(*args)\n"
        "    return wraps(function)(inner)\n"
        "def updates(function):\n"
        "    def inner(*args):\n        return function(*args)\n"
        "    update_wrapper(inner, function)\n"
        "    return inner\n"
        "def update_returned(function):\n"
        "    def inner(*args):\n        return function(*args)\n"
        "    return update_wrapper(inner, function)\n"
        "def both_arms(function):\n"
        "    if function:\n"
        "        def inner(*args):\n            return function(*args)\n"
        "    else:\n"
        "        def inner(*args):\n            return function(*args)\n"
        "    return wraps(function)(inner)\n"
        "def one_arm(function):\n"
        "    if function:\n"
        "        def inner(*args):\n            return function(*args)\n"
        "    else:\n"
        "        def inner(*args):\n            return None\n"
        "    return wraps(function)(inner)\n"
        "def one_arm_wrapped(function):\n"
        "    if function:\n"
        "        @wraps(function)\n"
        "        def inner(*args):\n            return function(*args)\n"
        "    else:\n"
        "        def inner(*args):\n            return None\n"
        "    return inner\n"
        "def hands_on(function):\n    return tagged('x')(function)\n"
        "def hands_on_direct(function):\n    return same(function)\n"
        "def hands_to_bad(function):\n    return replace(function)\n"
        "class only(classmethod):\n"
        "    def __get__(self, instance, owner=None):\n"
        "        return super().__get__(instance, owner)\n"
        "def same(function):\n    return function\n"
        "def logged(function):\n"
        "    @wraps(function)\n"
        "    def wrapper(*args, **kwargs):\n        return function(*args, **kwargs)\n"
        "    return wrapper\n"
        "def tagged(name):\n"
        "    def apply(function):\n        return function\n"
        "    return apply\n"
        "def either(*args, path=None):\n"
        "    def apply(klass):\n        return klass\n"
        "    if not args:\n        return apply\n"
        "    return apply(*args)\n"
        "def replace(function):\n    return 42\n"
        "def other():\n    return 2\n"
        "def replaces(function):\n    return other\n"
        "def factory(name):\n    def apply(function):\n        return function\n    return apply\n"
        "def stubbed(function): ...\n"
        "def raises(function):\n    raise NotImplementedError\n"
        "def nothing(function):\n    pass\n"
        "def opaque(function):\n    return make(function)\n"
        "def swallow(function):\n"
        "    @wraps(function)\n"
        "    def wrapper():\n        return 1\n"
        "    return wrapper\n"
        "def conditional(function):\n"
        "    if function:\n        return function\n"
        "def generated(function):\n    yield function\n"
        "class described:\n"
        "    def __init__(self, function):\n        self.function = function\n"
        "    def __get__(self, instance, owner=None):\n        return self.function(instance)\n"};
    static const struct {
        const char *decorator;
        int expected;
    } cases[] = {
        {"@same", 1},          {"@logged", 1},         {"@tagged('x')", 1},    {"@either", 1},
        {"@either(path='p')", 1}, {"@same\n@logged", 1}, {"@wraps_called", 1},  {"@updates", 1},
        {"@update_returned", 1}, {"@hands_on", 1},     {"@hands_on_direct", 1}, {"@both_arms", 1},
        /* A decorator it cannot see through still leaves the name on this declaration:
         * what runs is dispatch, and flask's @setupmethod and django's @register.filter
         * are exactly this. */
        {"@opaque", 1},        {"@swallow", 1},        {"@conditional", 1},
        {"@same('x')", 1},     {"@external_deco", 1},  {"@one_arm", 1},        {"@one_arm_wrapped", 1},
        /* The source proves the name stops holding the function. */
        {"@replace", 0},       {"@replaces", 0},       {"@factory('x')", 1},  {"@tagged", 0},
        {"@nothing", 0},        {"@generated", 0},      {"@same\n@replace", 0},
        {"@hands_to_bad", 1},  {"@stubbed", 1},        {"@raises", 1},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[768];
        snprintf(source, sizeof(source),
                 "from lib.deco import same, logged, tagged, either, replace, nothing, opaque, "
                 "swallow, conditional, generated, wraps_called, updates, update_returned, "
                 "hands_on, hands_on_direct, hands_to_bad, both_arms, one_arm, one_arm_wrapped, "
                 "stubbed, raises, replaces, factory\n"
                 "from external import external_deco\n"
                 "%s\ndef target():\n    return 1\ndef run():\n    return target()\n",
                 cases[i].decorator);
        const python_proof_file_t files[] = {{"main.py", source}, decorators};
        ASSERT(python_expect(cases[i].decorator, files, 2, "CALLS", "main.py", "target", "Function",
                             cases[i].expected));
    }
    /* A kept declaration is an ordinary export, and an ordinary base class. */
    EXPECT_CALL("kept across files", "lib/made.py", "target", "Function", 1,
                {"main.py", "from lib.made import target\ndef run():\n    return target()\n"},
                {"lib/made.py", "from lib.deco import logged\n@logged\ndef target():\n    return 1\n"},
                decorators);
    EXPECT_CALL("decorated base class", "lib/made.py", "save", "Method", 1,
                {"main.py", "from lib.made import Base\nclass Child(Base):\n"
                            "    def run(self):\n        return self.save()\n"},
                {"lib/made.py", "from lib.deco import either\n@either(path='x')\nclass Base:\n"
                                "    def save(self):\n        return 1\n"},
                decorators);
    EXPECT_CALL("decorated base in one file", "main.py", "save", "Method", 1,
                {"main.py", "from functools import total_ordering\n@total_ordering\nclass Base:\n"
                            "    def save(self):\n        return 1\n"
                            "class Child(Base):\n    def run(self):\n        return super().save()\n"});
    /* celery: @Thenable.register class AsyncResult(ResultBase). */
    EXPECT_CALL("base behind an unproven decorator", "main.py", "save", "Method", 1,
                {"main.py", "from external import wrap\n@wrap\nclass Base:\n"
                            "    def save(self):\n        return 1\n"
                            "class Child(Base):\n    def run(self):\n        return super().save()\n"});
    EXPECT_CALL("a base the decorator discards", "main.py", "save", "Method", 0,
                {"main.py", "from lib.deco import replace\n@replace\nclass Base:\n"
                            "    def save(self):\n        return 1\n"
                            "class Child(Base):\n    def run(self):\n        return super().save()\n"},
                decorators);
    /* flask: @setupmethod def add_url_rule, called on self from a subclass. */
    EXPECT_CALL("a method behind a decorator it cannot see through", "main.py", "go", "Method", 1,
                {"main.py", "from external import setup\nclass Base:\n    @setup\n"
                            "    def go(self):\n        return 1\n"
                            "class Child(Base):\n    def run(self):\n        return self.go()\n"});
    EXPECT_CALL("a method the decorator discards", "main.py", "go", "Method", 0,
                {"main.py", "from lib.deco import nothing\nclass Base:\n    @nothing\n"
                            "    def go(self):\n        return 1\n"
                            "    def run(self):\n        return self.go()\n"},
                decorators);
    /* celery: kombu's @cached_property, whose value is what the site calls. */
    EXPECT_CALL("a property by its decorator's name is not called", "main.py", "go", "Method", 0,
                {"main.py", "from elsewhere import cached_property\nclass A:\n"
                            "    @cached_property\n    def go(self):\n        return make()\n"
                            "    def run(self):\n        return self.go()\n"});
    EXPECT_CALL("classmethod subclass binds the function", "main.py", "go", "Method", 1,
                {"main.py", "from lib.deco import only\nclass A:\n    @only\n    def go(cls):\n"
                            "        return 1\ndef run():\n    return A.go()\n"},
                decorators);
    /* Standard library facts, and only for the real module. */
    EXPECT_CALL("stdlib cache", "main.py", "target", "Function", 1,
                {"main.py", "import functools\n@functools.lru_cache(maxsize=1)\ndef target():\n"
                            "    return 1\ndef run():\n    return target()\n"});
    EXPECT_CALL("stdlib abstract method", "main.py", "go", "Method", 1,
                {"main.py", "from abc import abstractmethod\nclass A:\n    @abstractmethod\n"
                            "    def go(self):\n        return 1\n"
                            "    def run(self):\n        return self.go()\n"});
    EXPECT_CALL("project module named like stdlib", "main.py", "target", "Function", 0,
                {"main.py", "from lib.functools import lru_cache\n@lru_cache\ndef target():\n"
                            "    return 1\ndef run():\n    return target()\n"},
                {"lib/functools.py", "def lru_cache(function):\n    return 42\n"});
    /* A property runs on read: a read references it, and calling the result does not call it. */
    {
        const char *source = "class A:\n    @property\n    def value(self):\n        return 1\n"
                             "    def read(self):\n        return self.value\n";
        const python_proof_file_t files[] = {{"main.py", source}};
        ASSERT(python_expect("property read", files, 1, "USAGE", "main.py", "value", "Method", 1));
        /* A getter and setter are one property, and the one node stands for it. */
        const char *paired = "class A:\n    @property\n    def value(self):\n        return 1\n"
                             "    @value.setter\n    def value(self, item):\n        pass\n"
                             "    def read(self):\n        return self.value\n";
        const python_proof_file_t pair[] = {{"main.py", paired}};
        ASSERT(python_expect("getter and setter share one node", pair, 1, "USAGE", "main.py", "value",
                             "Method", 1));
        const char *argument = "class A:\n    @property\n    def value(self):\n        return 1\n"
                               "    def plain(self):\n        return 2\n"
                               "    def use(self, check):\n"
                               "        check(self.value)\n        check(self.plain)\n";
        const python_proof_file_t passed[] = {{"main.py", argument}};
        ASSERT(python_expect("property argument is a read", passed, 1, "USAGE", "main.py", "value", "Method", 1));
        ASSERT(python_expect("property argument is no reference", passed, 1, "CALL_REFERENCE", "main.py", "value", "Method", 0));
        ASSERT(python_expect("method argument is a reference", passed, 1, "CALL_REFERENCE", "main.py", "plain", "Method", 1));
        const char *call = "class A:\n    @property\n    def value(self):\n        return other\n"
                           "    def use(self):\n        return self.value()\n";
        const python_proof_file_t called[] = {{"main.py", call}};
        ASSERT(python_expect("property result called", called, 1, "CALLS", "main.py", "value", "Method", 0));
    }
    {
        const char *source = "from lib.deco import described\nclass A:\n    @described\n"
                             "    def value(self):\n        return 1\n"
                             "    def read(self):\n        return self.value\n"
                             "    def use(self):\n        return self.value()\n";
        const python_proof_file_t files[] = {{"main.py", source}, decorators};
        ASSERT(python_expect("descriptor read", files, 2, "USAGE", "main.py", "value", "Method", 1));
        ASSERT(python_expect("descriptor call", files, 2, "CALLS", "main.py", "value", "Method", 0));
    }
    /* A decorator whose source nothing shows keeps the declaration, as on a function. */
    EXPECT_CALL("unknown method decorator", "main.py", "go", "Method", 1,
                {"main.py", "from external import wrap\nclass A:\n    @wrap\n    def go(self):\n"
                            "        return 1\n    def run(self):\n        return self.go()\n"});
    PASS();
}

/* Five suites because each case indexes through both drivers: together they ran past the
 * 900 s per-suite cap on Windows, about 500 full indexes in all. */
/* ── Stated source roots ──────────────────────────────────────────────────
 * Forms from click, flask and poetry: a src layout reached from tests and examples, and a
 * namespace package kept without __init__.py. Without a manifest nothing states the layout. */
TEST(resolver_python_stated_source_root_reaches_the_module) {
    static const char echo[] = "def echo():\n    return 1\n";
    static const char caller[] = "import click\ndef run():\n    click.echo()\n";
    {
        const python_proof_file_t files[] = {
            {"pyproject.toml", "[project]\nname = \"click\"\n"},
            {"src/click/__init__.py", "from .utils import echo as echo\n"},
            {"src/click/utils.py", echo},
            {"tests/test_utils.py", caller},
            {"examples/colors/colors.py", caller},
            {"conftest.py", caller}};
        ASSERT(python_expect_from("a test reaches src by name", files, 6, "tests/test_utils.py",
                                  "CALLS", "src/click/utils.py", "echo", "Function", 1));
        ASSERT(python_expect_from("an example reaches src by name", files, 6,
                                  "examples/colors/colors.py", "CALLS", "src/click/utils.py",
                                  "echo", "Function", 1));
        ASSERT(python_expect_from("a root file reaches src by name", files, 6, "conftest.py",
                                  "CALLS", "src/click/utils.py", "echo", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {{"src/click/__init__.py",
                                              "from .utils import echo as echo\n"},
                                             {"src/click/utils.py", echo},
                                             {"tests/test_utils.py", caller}};
        ASSERT(python_expect_from("no manifest states the layout", files, 3, "tests/test_utils.py",
                                  "CALLS", "src/click/utils.py", "echo", "Function", 0));
    }
    {
        /* flask/examples: a project of its own, depending on the enclosing one. */
        const python_proof_file_t files[] = {
            {"pyproject.toml", "[project]\nname = \"Flask\"\n"},
            {"src/flask/__init__.py", "from .app import Flask as Flask\n"},
            {"src/flask/app.py", "class Flask:\n    pass\n"},
            {"examples/celery/pyproject.toml", "[project]\nname = \"flask-example-celery\"\n"},
            {"examples/celery/src/task_app/__init__.py",
             "from flask import Flask\nfrom .views import home\ndef create_app():\n"
             "    home()\n    return Flask()\n"},
            {"examples/celery/src/task_app/views.py", echo},
            {"examples/celery/tests/test_app.py",
             "from task_app.views import echo\ndef run():\n    echo()\n"}};
        ASSERT(python_expect_from("an example reaches the enclosing project", files, 7,
                                  "examples/celery/src/task_app/__init__.py", "CALLS",
                                  "src/flask/app.py", "Flask", "Class", 1));
        ASSERT(python_expect_from("an example's test reaches its own project", files, 7,
                                  "examples/celery/tests/test_app.py", "CALLS",
                                  "examples/celery/src/task_app/views.py", "echo", "Function", 1));
    }
    PASS();
}

TEST(resolver_python_namespace_package_under_a_source_root) {
    /* poetry: src/poetry has no __init__.py and src/poetry/poetry.py shares its name. */
    const python_proof_file_t files[] = {
        {"pyproject.toml", "[project]\nname = \"poetry\"\n"},
        {"src/poetry/poetry.py", "class Poetry:\n    pass\n"},
        {"src/poetry/config/__init__.py", ""},
        {"src/poetry/config/source.py", "def split_key():\n    return 1\n"},
        {"src/poetry/config/config.py",
         "from poetry.config.source import split_key\nfrom poetry.poetry import Poetry\n"
         "from poetry.core.packages import Package\nfrom config.source import split_key as near\n"
         "def merge():\n    split_key()\n    Poetry()\n    Package()\n"},
        {"src/poetry/config/other.py",
         "from config.source import split_key\ndef merge():\n    split_key()\n"},
        {"src/poetry/__main__.py",
         "from poetry.config.source import split_key\ndef main():\n    split_key()\n"},
        {"tests/helpers.py", "def helper():\n    return 1\n"},
        {"tests/test_config.py",
         "from helpers import helper\nfrom poetry.config.source import split_key\n"
         "def run():\n    helper()\n    split_key()\n"}};
    ASSERT(python_expect_from("own namespace package", files, 9, "src/poetry/config/config.py",
                              "CALLS", "src/poetry/config/source.py", "split_key", "Function", 1));
    ASSERT(python_expect_from("own namespace package, the same-named module", files, 9,
                              "src/poetry/config/config.py", "CALLS", "src/poetry/poetry.py",
                              "Poetry", "Class", 1));
    ASSERT(python_expect_from("a package's parent is not on sys.path below a root", files, 9,
                              "src/poetry/config/other.py", "CALLS", "src/poetry/config/source.py",
                              "split_key", "Function", 0));
    ASSERT(python_expect_from("a module directly in the namespace package", files, 9,
                              "src/poetry/__main__.py", "CALLS", "src/poetry/config/source.py",
                              "split_key", "Function", 1));
    ASSERT(python_expect_from("a test reaches the namespace package", files, 9,
                              "tests/test_config.py", "CALLS", "src/poetry/config/source.py",
                              "split_key", "Function", 1));
    ASSERT(python_expect_from("a test's neighbour stays first", files, 9, "tests/test_config.py",
                              "CALLS", "tests/helpers.py", "helper", "Function", 1));
    PASS();
}

/* Each form a backend reads, and one decoy root it must not take from the file's other text. */
TEST(resolver_python_manifests_state_their_roots) {
    static const char go[] = "def go():\n    return 1\n";
    static const char caller[] = "import pkg.inner\nfrom pkg.inner import go\ndef run():\n    go()\n";
    static const struct {
        const char *what, *manifest, *text, *module;
        int expected;
    } forms[] = {
        {"setuptools where", "pyproject.toml",
         "[tool.setuptools.packages.find]\nwhere = [\"lib\"]\n", "lib/pkg/inner.py", 1},
        {"setuptools package-dir", "pyproject.toml",
         "[tool.setuptools]\npackage-dir = {\"\" = \"lib\"}\n", "lib/pkg/inner.py", 1},
        {"setuptools package-dir table", "pyproject.toml",
         "[tool.setuptools.package-dir]\n\"\" = \"lib\"\n", "lib/pkg/inner.py", 1},
        {"poetry from", "pyproject.toml",
         "[tool.poetry]\npackages = [\n  { include = \"pkg\", from = \"lib\" }, # \"nope\"\n]\n",
         "lib/pkg/inner.py", 1},
        {"poetry from as a table", "pyproject.toml",
         "[[tool.poetry.packages]]\ninclude = \"pkg\"\nfrom = \"lib\"\n", "lib/pkg/inner.py", 1},
        {"hatch packages", "pyproject.toml",
         "[tool.hatch.build.targets.wheel]\npackages = [\"lib/pkg\"]\n", "lib/pkg/inner.py", 1},
        {"pdm package-dir", "pyproject.toml", "[tool.pdm.build]\npackage-dir = \"lib\"\n",
         "lib/pkg/inner.py", 1},
        {"maturin python-source", "pyproject.toml", "[tool.maturin]\npython-source = \"lib\"\n",
         "lib/pkg/inner.py", 1},
        {"pytest pythonpath", "pyproject.toml",
         "[tool.pytest.ini_options]\npythonpath = [\"lib\", \".\"]\n", "lib/pkg/inner.py", 1},
        {"ruff src", "pyproject.toml", "[tool.ruff]\nsrc = [\"lib\"]\n", "lib/pkg/inner.py", 1},
        {"pyright extraPaths", "pyproject.toml", "[tool.pyright]\nextraPaths = [\"lib\"]\n",
         "lib/pkg/inner.py", 1},
        {"pytest.ini pythonpath", "pytest.ini", "[pytest]\npythonpath = lib\n", "lib/pkg/inner.py",
         1},
        {"tox.ini pythonpath", "tox.ini", "[pytest]\npythonpath = lib\n", "lib/pkg/inner.py", 1},
        {"setup.cfg package_dir", "setup.cfg", "[options]\npackage_dir =\n    = lib\n",
         "lib/pkg/inner.py", 1},
        {"setup.cfg where", "setup.cfg", "[options.packages.find]\nwhere = lib\n",
         "lib/pkg/inner.py", 1},
        {"setup.py package_dir", "setup.py", "setup(package_dir={\"\": \"lib\"})\n",
         "lib/pkg/inner.py", 1},
        {"setup.py find_packages", "setup.py", "setup(packages=find_packages(where='lib'))\n",
         "lib/pkg/inner.py", 1},
        {"convention beside setup.py", "setup.py", "setup()\n", "src/pkg/inner.py", 1},
        {"a key elsewhere states nothing", "pyproject.toml",
         "[tool.other]\nwhere = [\"lib\"]\n", "lib/pkg/inner.py", 0},
        {"another package's mapping states nothing", "setup.cfg",
         "[options]\npackage_dir =\n    other = lib\n", "lib/pkg/inner.py", 0},
        {"a commented root states nothing", "pyproject.toml",
         "[tool.setuptools.packages.find]\nwhere = [\n  # \"lib\"\n  \"elsewhere\",\n]\n",
         "lib/pkg/inner.py", 0},
    };
    for (size_t i = 0; i < sizeof(forms) / sizeof(forms[0]); i++) {
        char init[64];
        snprintf(init, sizeof(init), "%.*s__init__.py",
                 (int)(strrchr(forms[i].module, '/') - forms[i].module + 1), forms[i].module);
        const python_proof_file_t files[] = {{forms[i].manifest, forms[i].text},
                                             {init, ""},
                                             {forms[i].module, go},
                                             {"tests/test_a.py", caller}};
        ASSERT(python_expect_from(forms[i].what, files, 4, "tests/test_a.py", "CALLS",
                                  forms[i].module, "go", "Function", forms[i].expected));
    }
    PASS();
}

/* pydantic-ai's shape: members of one uv workspace import each other. */
TEST(resolver_python_workspace_members_see_each_other) {
    static const char caller[] = "from beta.inner import go\ndef run():\n    go()\n";
    static const char go[] = "def go():\n    return 1\n";
    static const char workspace[] = "[tool.uv.workspace]\nmembers = [\"packages/*\", \"extra\"]\n";
    static const char member[] = "[project]\nname = \"member\"\n";
    {
        const python_proof_file_t files[] = {{"pyproject.toml", workspace},
                                             {"packages/alpha/pyproject.toml", member},
                                             {"packages/alpha/src/alpha/__init__.py", ""},
                                             {"packages/alpha/src/alpha/user.py", caller},
                                             {"packages/beta/pyproject.toml", member},
                                             {"packages/beta/src/beta/__init__.py", ""},
                                             {"packages/beta/src/beta/inner.py", go}};
        ASSERT(python_expect_from("a sibling member's src layout", files, 7,
                                  "packages/alpha/src/alpha/user.py", "CALLS",
                                  "packages/beta/src/beta/inner.py", "go", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {{"pyproject.toml", workspace},
                                             {"extra/pyproject.toml", member},
                                             {"extra/beta/__init__.py", ""},
                                             {"extra/beta/inner.py", go},
                                             {"packages/alpha/pyproject.toml", member},
                                             {"packages/alpha/alpha/__init__.py", ""},
                                             {"packages/alpha/alpha/user.py", caller}};
        ASSERT(python_expect_from("an exact member, flat", files, 7, "packages/alpha/alpha/user.py",
                                  "CALLS", "extra/beta/inner.py", "go", "Function", 1));
    }
    {
        /* Not listed: another project that happens to be in the tree. */
        const python_proof_file_t files[] = {{"pyproject.toml", workspace},
                                             {"packages/alpha/pyproject.toml", member},
                                             {"packages/alpha/alpha/__init__.py", ""},
                                             {"packages/alpha/alpha/user.py", caller},
                                             {"examples/beta_demo/pyproject.toml", member},
                                             {"examples/beta_demo/beta/__init__.py", ""},
                                             {"examples/beta_demo/beta/inner.py", go}};
        ASSERT(python_expect_from("a project outside the members", files, 7,
                                  "packages/alpha/alpha/user.py", "CALLS",
                                  "examples/beta_demo/beta/inner.py", "go", "Function", 0));
    }
    {
        /* Two members provide the name: nothing says which. */
        const python_proof_file_t files[] = {{"pyproject.toml", workspace},
                                             {"packages/alpha/pyproject.toml", member},
                                             {"packages/alpha/alpha/__init__.py", ""},
                                             {"packages/alpha/alpha/user.py", caller},
                                             {"packages/one/pyproject.toml", member},
                                             {"packages/one/beta/__init__.py", ""},
                                             {"packages/one/beta/inner.py", go},
                                             {"packages/two/pyproject.toml", member},
                                             {"packages/two/beta/__init__.py", ""},
                                             {"packages/two/beta/inner.py", go}};
        ASSERT(python_expect_from("two members of one name", files, 10,
                                  "packages/alpha/alpha/user.py", "CALLS",
                                  "packages/one/beta/inner.py", "go", "Function", 0));
    }
    {
        /* No workspace declared: siblings stay out of reach. */
        const python_proof_file_t files[] = {{"packages/alpha/pyproject.toml", member},
                                             {"packages/alpha/alpha/__init__.py", ""},
                                             {"packages/alpha/alpha/user.py", caller},
                                             {"packages/beta/pyproject.toml", member},
                                             {"packages/beta/beta/__init__.py", ""},
                                             {"packages/beta/beta/inner.py", go}};
        ASSERT(python_expect_from("siblings without a workspace", files, 6,
                                  "packages/alpha/alpha/user.py", "CALLS",
                                  "packages/beta/beta/inner.py", "go", "Function", 0));
    }
    PASS();
}

TEST(resolver_python_source_root_decoys_abstain) {
    static const char go[] = "def go():\n    return 1\n";
    static const char manifest[] = "[project]\nname = \"demo\"\n";
    static const char caller[] = "import helper\ndef run():\n    helper.go()\n";
    {
        /* A src directory that is a package holds src.helper, not helper. */
        const python_proof_file_t files[] = {{"pyproject.toml", manifest},
                                             {"src/__init__.py", ""},
                                             {"src/helper.py", go},
                                             {"tests/test_a.py", caller}};
        ASSERT(python_expect_from("src is itself a package", files, 4, "tests/test_a.py", "CALLS",
                                  "src/helper.py", "go", "Function", 0));
    }
    {
        /* The standard library comes before an installed distribution. */
        const python_proof_file_t files[] = {
            {"pyproject.toml", manifest},
            {"src/logging.py", "def getLogger():\n    return 1\n"},
            {"tests/test_a.py", "import logging\ndef run():\n    logging.getLogger()\n"}};
        ASSERT(python_expect_from("a standard library name", files, 3, "tests/test_a.py", "CALLS",
                                  "src/logging.py", "getLogger", "Function", 0));
    }
    {
        const python_proof_file_t files[] = {{"pyproject.toml", manifest},
                                             {"helper.py", go},
                                             {"src/helper.py", go},
                                             {"tests/test_a.py", caller}};
        /* The repository root comes first, as it does for a type checker. */
        ASSERT(python_expect_from("root and src both have it, root", files, 4, "tests/test_a.py",
                                  "CALLS", "helper.py", "go", "Function", 1));
        ASSERT(python_expect_from("root and src both have it, src", files, 4, "tests/test_a.py",
                                  "CALLS", "src/helper.py", "go", "Function", 0));
    }
    {
        const python_proof_file_t files[] = {{"sdk/pyproject.toml", manifest},
                                             {"sdk/helper.py", go},
                                             {"sdk/src/helper.py", go},
                                             {"sdk/tests/test_a.py", caller}};
        ASSERT(python_expect_from("flat and src both have it, flat", files, 4,
                                  "sdk/tests/test_a.py", "CALLS", "sdk/helper.py", "go", "Function",
                                  0));
        ASSERT(python_expect_from("flat and src both have it, src", files, 4, "sdk/tests/test_a.py",
                                  "CALLS", "sdk/src/helper.py", "go", "Function", 0));
    }
    {
        /* A flat project in a subdirectory: its own directory is the root, and its scripts
         * still see their neighbours first. */
        const python_proof_file_t files[] = {
            {"sdk/python/pyproject.toml", manifest},
            {"sdk/python/mypkg/__init__.py", ""},
            {"sdk/python/mypkg/inner.py", go},
            {"sdk/python/tests/test_a.py", "from mypkg.inner import go\ndef run():\n    go()\n"},
            {"sdk/python/scripts/helper.py", "def near():\n    return 1\n"},
            {"sdk/python/scripts/run.py", "from helper import near\ndef run():\n    near()\n"},
            {"docs/conf.py", "from mypkg.inner import go\ndef run():\n    go()\n"}};
        ASSERT(python_expect_from("a flat project in a subdirectory", files, 7,
                                  "sdk/python/tests/test_a.py", "CALLS",
                                  "sdk/python/mypkg/inner.py", "go", "Function", 1));
        ASSERT(python_expect_from("its script sees its neighbour", files, 7,
                                  "sdk/python/scripts/run.py", "CALLS",
                                  "sdk/python/scripts/helper.py", "near", "Function", 1));
        ASSERT(python_expect_from("outside that project it is not on the path", files, 7,
                                  "docs/conf.py", "CALLS", "sdk/python/mypkg/inner.py", "go",
                                  "Function", 0));
    }
    {
        /* A nearer project's module of that name hides the enclosing project's. */
        const python_proof_file_t files[] = {
            {"pyproject.toml", manifest},
            {"src/shared/__init__.py", ""},
            {"src/shared/inner.py", go},
            {"sub/pyproject.toml", manifest},
            {"sub/src/shared/__init__.py", ""},
            {"sub/tests/test_a.py", "from shared.inner import go\ndef run():\n    go()\n"}};
        ASSERT(python_expect_from("a nearer project hides the enclosing one", files, 6,
                                  "sub/tests/test_a.py", "CALLS", "src/shared/inner.py", "go",
                                  "Function", 0));
    }
    PASS();
}

/* ── What changes a module's namespace ────────────────────────────────────
 * Forms from django: a test's `django.__file__ = None` beside `from django import forms`,
 * and `from . import signals` in a package's own __init__. */
TEST(resolver_python_only_a_store_of_that_name_blocks_a_submodule) {
    static const python_proof_file_t child = {"pkg/mod.py", "def real():\n    return 1\n"};
    static const char main_py[] = "from pkg import mod\ndef run():\n    return mod.real()\n";
    static const struct {
        const char *patcher;
        int expected;
    } cases[] = {
        /* Handing a module on, and storing some other name, select no other declaration. */
        {"import pkg\ndef patch():\n    install(pkg)\n", 1},
        {"import pkg\nsaved = pkg\n", 1},
        {"import pkg\nalias = pkg\nalias.__file__ = None\n", 1},
        {"import pkg\npkg.__file__ = None\n", 1},
        {"import pkg\ndel pkg.__file__\n", 1},
        {"import pkg\nsetattr(pkg, '__file__', None)\n", 1},
        {"import pkg\nwith patch.object(pkg, 'mod', None):\n    pass\n", 1},
        {"import pkg.mod\nimport pkg.other\npkg.other.value = 1\n", 1},
        /* The name itself. */
        {"import pkg\npkg.mod = None\n", 0},
        {"import pkg\ndel pkg.mod\n", 0},
        {"import pkg\nsetattr(pkg, 'mod', None)\n", 0},
        {"import pkg\ndelattr(pkg, 'mod')\n", 0},
        {"import pkg\npkg.mod, other = 1, 2\n", 0},
        {"import pkg\nalias = pkg\nalias.mod = None\n", 0},
        {"import pkg\nfirst = pkg\nsecond = first\nsecond.mod = None\n", 0},
        /* Anything could be the name. */
        {"import pkg\nsetattr(pkg, choose(), None)\n", 0},
        {"import pkg\npkg.__dict__['mod'] = None\n", 0},
        {"import pkg\nvars(pkg)['mod'] = None\n", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const python_proof_file_t files[] = {{"pkg/__init__.py", ""},
                                             child,
                                             {"pkg/other.py", "value = 0\n"},
                                             {"patcher.py", cases[i].patcher},
                                             {"main.py", main_py}};
        ASSERT(python_expect(cases[i].patcher, files, 5, "CALLS", "pkg/mod.py", "real", "Function",
                             cases[i].expected));
    }
    PASS();
}

TEST(resolver_python_writes_through_the_submodule_leave_its_identity) {
    /* tests/mail: `del django.conf.settings.X` and mock.patch.object(django.utils.timezone, ...)
     * change members; neither changes which module `from django.utils import timezone` is. */
    const python_proof_file_t files[] = {
        {"pkg/__init__.py", ""},
        {"pkg/mod.py", "def real():\n    return 1\n"},
        {"tests/patcher.py", "import pkg.mod\ndel pkg.mod.settings\npkg.mod.real = None\n"},
        {"main.py", "from pkg import mod\ndef run():\n    return mod.real()\n"}};
    ASSERT(python_expect("a store inside the child", files, 4, "CALLS", "pkg/mod.py", "real",
                         "Function", 1));
    PASS();
}

/* click/__init__.py has a __getattr__ for its deprecations, and `from .types import X` above it. */
TEST(resolver_python_module_getattr_only_answers_what_is_missing) {
    static const char go[] = "def real():\n    return 1\n";
    static const char user[] = "from pkg import mod\ndef run():\n    return mod.real()\n";
    static const char dotted[] = "import pkg\ndef run():\n    return pkg.mod.real()\n";
    static const char lazy[] = "def __getattr__(name):\n    return load(name)\n";
    {
        char init[128];
        snprintf(init, sizeof(init), "from .mod import real as real\n%s", lazy);
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", init}, {"pkg/mod.py", go}, {"user.py", user}, {"dotted.py", dotted}};
        ASSERT(python_expect_from("a child __init__ loads, by from-import", files, 4, "user.py",
                                  "CALLS", "pkg/mod.py", "real", "Function", 1));
        ASSERT(python_expect_from("a child __init__ loads, by attribute", files, 4, "dotted.py",
                                  "CALLS", "pkg/mod.py", "real", "Function", 1));
    }
    {
        char init[128];
        snprintf(init, sizeof(init), "from . import mod\n%s", lazy);
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", init}, {"pkg/mod.py", go}, {"user.py", user}, {"dotted.py", dotted}};
        ASSERT(python_expect_from("a child __init__ binds by from-dot", files, 4, "user.py", "CALLS",
                                  "pkg/mod.py", "real", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", lazy}, {"pkg/mod.py", go}, {"user.py", user}, {"dotted.py", dotted}};
        ASSERT(python_expect_from("a child only __getattr__ could answer for", files, 4, "user.py",
                                  "CALLS", "pkg/mod.py", "real", "Function", 0));
    }
    PASS();
}

TEST(resolver_python_package_importing_its_own_child) {
    static const char go[] = "def go():\n    return 1\n";
    static const char user[] = "from pkg import mod\ndef run():\n    return mod.go()\n";
    static const char direct[] = "import pkg\ndef run():\n    return pkg.mod.go()\n";
    static const struct {
        const char *init;
        int expected;
    } cases[] = {
        {"from . import mod\n", 1},
        {"from pkg import mod\n", 1},
        {"from . import mod, other\n", 1},
        /* The statement is not the name's only binder. */
        {"mod = None\nfrom . import mod\n", 0},
        {"from . import mod\nmod = None\n", 0},
        {"from .other import mod\n", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const python_proof_file_t files[] = {{"pkg/__init__.py", cases[i].init},
                                             {"pkg/mod.py", go},
                                             {"pkg/other.py", "mod = 1\n"},
                                             {"user.py", user},
                                             {"main.py", direct}};
        ASSERT(python_expect_from(cases[i].init, files, 5, "user.py", "CALLS", "pkg/mod.py", "go",
                                  "Function", cases[i].expected));
        ASSERT(python_expect_from(cases[i].init, files, 5, "main.py", "CALLS", "pkg/mod.py", "go",
                                  "Function", cases[i].expected));
    }
    PASS();
}

/* sqlalchemy's package inits end with `__go(locals())`. Handing the namespace to the module's
 * own def writes nothing when that def never spells the parameter, nor reaches it another way;
 * what the body does by other means is seen as anywhere else. */
TEST(resolver_python_package_init_hands_its_namespace_on) {
    static const char make[] = "def make():\n    return 1\n";
    static const char user[] = "from pkg import make\ndef run():\n    return make()\n";
    static const char direct[] = "import pkg\ndef run():\n    return pkg.make()\n";
    static const struct {
        const char *tail;
        int expected;
    } cases[] = {
        /* lib/sqlalchemy/orm/__init__.py */
        {"from typing import Any\nfrom . import other as _sa_util\n\n\n"
         "def __go(lcls: Any) -> None:\n    _sa_util.preloaded.import_prefix(\"pkg\")\n\n\n"
         "__go(locals())\n",
         1},
        /* lib/sqlalchemy/__init__.py */
        {"def __go(lcls):\n    from . import other\n\n    other._token = \"\".join([])\n\n\n"
         "__go(locals())\n",
         1},
        /* lib/sqlalchemy/orm/collections.py: globals the body rebinds are its own names */
        {"def __go(lcls):\n    global keyfunc\n    from .other import keyfunc\n\n\n"
         "__go(locals())\n",
         1},
        {"def __go(lcls):\n    pass\n\n\nresult = __go(vars())\n", 1},
        /* The body reaches the namespace it was given. */
        {"def __go(lcls):\n    lcls['make'] = None\n\n\n__go(locals())\n", 0},
        {"def __go(lcls):\n    helper(lcls)\n\n\ndef helper(d):\n    pass\n\n\n__go(locals())\n",
         0},
        {"def __go(lcls):\n    locals()['lcls']['make'] = None\n\n\n__go(locals())\n", 0},
        {"def __go(lcls):\n    eval(\"lcls.clear()\")\n\n\n__go(locals())\n", 0},
        {"import sys\n\n\ndef __go(lcls):\n    sys._getframe().f_locals['lcls'].clear()\n\n\n"
         "__go(locals())\n",
         0},
        {"def __go(lcls):\n    def inner():\n        lcls.clear()\n    inner()\n\n\n"
         "__go(locals())\n",
         0},
        {"def __go(first, lcls=None):\n    first['make'] = None\n\n\n__go(locals())\n", 0},
        {"def __go(*args):\n    args[0]['make'] = None\n\n\n__go(locals())\n", 0},
        {"def __go(lcls):\n    global make\n    make = None\n\n\n__go(locals())\n", 0},
        /* The callee is not provably this module's one plain def at the call. */
        {"from .other import __go\n__go(locals())\n", 0},
        {"def __go(lcls):\n    pass\n\n\n__go = print\n__go(locals())\n", 0},
        {"__go(locals())\n\n\ndef __go(lcls):\n    pass\n", 0},
        {"def wrap(f):\n    return f\n\n\n@wrap\ndef __go(lcls):\n    pass\n\n\n__go(locals())\n",
         0},
        {"def __go(lcls):\n    pass\n\n\nfrom .other import *\n__go(locals())\n", 0},
        {"def __go(other, lcls):\n    pass\n\n\n__go(1, locals())\n", 0},
        {"__go = lambda lcls: None\n__go(locals())\n", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char init[640];
        snprintf(init, sizeof(init), "from .impl import make\n%s", cases[i].tail);
        const python_proof_file_t files[] = {{"pkg/__init__.py", init},
                                             {"pkg/impl.py", make},
                                             {"pkg/other.py", "keyfunc = 1\n__all__ = []\n"},
                                             {"user.py", user},
                                             {"main.py", direct}};
        ASSERT(python_expect_from(cases[i].tail, files, 5, "user.py", "CALLS", "pkg/impl.py",
                                  "make", "Function", cases[i].expected));
        ASSERT(python_expect_from(cases[i].tail, files, 5, "main.py", "CALLS", "pkg/impl.py",
                                  "make", "Function", cases[i].expected));
    }
    PASS();
}

/* ── What a call of a class, and of an unannotated method, gives ──────────
 * Forms from django and celery. */
TEST(resolver_python_special_methods_that_read_through_the_base) {
    /* django.utils.functional.LazyObject: answers through super, declared members stand. */
    EXPECT_CALL("__getattribute__ reading through super", "main.py", "go", "Method", 1,
                {"main.py",
                 "class Lazy:\n    def __getattribute__(self, name):\n"
                 "        if name == '_wrapped':\n"
                 "            return super().__getattribute__(name)\n"
                 "        value = super().__getattribute__(name)\n        return value\n"
                 "    def go(self):\n        return 1\n"
                 "    def run(self):\n        return self.go()\n"});
    EXPECT_CALL("__getattribute__ answering by itself", "main.py", "go", "Method", 0,
                {"main.py", "class Lazy:\n    def __getattribute__(self, name):\n"
                            "        return name\n    def go(self):\n        return 1\n"
                            "    def run(self):\n        return self.go()\n"});
    /* django Operation.__new__ allocates; celery chain.__new__ allocates on one path. */
    static const char *allocating[] = {
        "        self = object.__new__(cls)\n        return self\n",
        "        return super().__new__(cls)\n",
        "        return super(Made, cls).__new__(cls)\n",
        "        if args:\n            return reduce(args)\n        return super().__new__(cls)\n"};
    for (size_t i = 0; i < sizeof(allocating) / sizeof(allocating[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "class Made:\n    def __new__(cls, *args):\n%s"
                 "    def go(self):\n        return 1\n"
                 "def run():\n    made = Made()\n    return made.go()\n",
                 allocating[i]);
        const python_proof_file_t file = {"main.py", source};
        ASSERT(python_expect(allocating[i], &file, 1, "CALLS", "main.py", "go", "Method", 1));
    }
    EXPECT_CALL("__new__ that never allocates", "main.py", "go", "Method", 0,
                {"main.py", "class Made:\n    def __new__(cls):\n        return other\n"
                            "    def go(self):\n        return 1\n"
                            "def run():\n    made = Made()\n    return made.go()\n"});
    PASS();
}

TEST(resolver_python_returns_follow_the_receiver) {
    static const char signature[] =
        "class Signature:\n    def set(self, **options):\n        return self\n"
        "    def clone(self):\n        return copy.copy(self)\n"
        "    def apply(self):\n        return 1\n"
        "    @classmethod\n    def make(cls):\n        return cls()\n"
        "class Chord(Signature):\n    def apply(self):\n        return 2\n";
    char source[768];
    /* celery: chord(...).set(...) is a chord, although set is declared on Signature. */
    snprintf(source, sizeof(source), "import copy\n%sdef run():\n    return Chord().set().apply()\n",
             signature);
    {
        const python_proof_file_t file = {"main.py", source};
        python_route_release(&python_routes[0]);
        python_route_release(&python_routes[1]);
        ASSERT(python_expect("return self gives the receiver", &file, 1, "CALLS", "main.py",
                             "apply", "Method", 1));
        ASSERT_EQ(python_route_edges(&file, 1, "main.py", "CALLS", "main.py", "set", "Method",
                                     false),
                  1);
    }
    {
        /* Two files, so the two `apply`s can be told apart. */
        const python_proof_file_t files[] = {
            {"sig.py", "class Signature:\n    def set(self):\n        return self\n"
                       "    def apply(self):\n        return 1\n"},
            {"main.py", "from sig import Signature\nclass Chord(Signature):\n"
                        "    def apply(self):\n        return 2\n"
                        "def run():\n    return Chord().set().apply()\n"}};
        ASSERT(python_expect("return self selects the subclass's method", files, 2, "CALLS",
                             "main.py", "apply", "Method", 1));
        ASSERT(python_expect("return self does not select the base's", files, 2, "CALLS", "sig.py",
                             "apply", "Method", 0));
    }
    /* django: BaseExpression.copy returns copy.copy(self). */
    EXPECT_CALL("copy.copy is its argument", "main.py", "only", "Method", 1,
                {"main.py", "import copy\nclass A:\n    def clone(self):\n"
                            "        return copy.copy(self)\n    def only(self):\n"
                            "        return 1\n"
                            "def run():\n    return A().clone().only()\n"});
    EXPECT_CALL("a project copy is not the standard library's", "main.py", "only", "Method", 0,
                {"main.py", "from lib import copy\nclass A:\n    def clone(self):\n"
                            "        return copy(self)\n    def only(self):\n"
                            "        return 1\n"
                            "def run():\n    return A().clone().only()\n"},
                {"lib.py", "def copy(value):\n    return 1\n"});
    /* django: ModelAdmin.get_changelist returns the class, get_changelist_instance calls it. */
    EXPECT_CALL("a method returning a class", "main.py", "filters", "Method", 1,
                {"main.py", "class Listing:\n    def filters(self):\n        return 1\n"
                            "class Admin:\n    def kind(self):\n        return Listing\n"
                            "    def make(self):\n        kind = self.kind()\n"
                            "        return kind()\n"
                            "def run():\n    return Admin().make().filters()\n"});
    /* django.contrib.sitemaps: an unannotated property's value is what it returns. */
    EXPECT_CALL("an unannotated property", "main.py", "page", "Method", 1,
                {"main.py", "class Paginator:\n    def page(self):\n        return 1\n"
                            "class Sitemap:\n    @property\n    def paginator(self):\n"
                            "        return Paginator()\n"
                            "    def run(self):\n        return self.paginator.page()\n"});
    EXPECT_CALL("a property whose returns disagree", "main.py", "page", "Method", 0,
                {"main.py", "class Paginator:\n    def page(self):\n        return 1\n"
                            "class Other:\n    def page(self):\n        return 2\n"
                            "class Sitemap:\n    @property\n    def paginator(self):\n"
                            "        if self:\n            return Paginator()\n"
                            "        return Other()\n"
                            "    def run(self):\n        return self.paginator.page()\n"});
    PASS();
}

/* ── Which binder reaches a site ──────────────────────────────────────────
 * Forms from django.utils.archive, django.db.models.base and contrib.postgres.constraints. */
TEST(resolver_python_with_and_except_bind_what_they_say) {
    static const char archive[] =
        "class Archive:\n    def __enter__(self):\n        return self\n"
        "    def __exit__(self, *args):\n        return None\n"
        "    def extract(self):\n        return 1\n"
        "class Other:\n    def extract(self):\n        return 2\n"
        "class Shut:\n    def __enter__(self):\n        return Other()\n"
        "    def __exit__(self, *args):\n        return None\n";
    char source[1024];
    snprintf(source, sizeof(source),
             "%sdef run():\n    with Archive() as archive:\n        archive.extract()\n", archive);
    {
        const python_proof_file_t file = {"main.py", source};
        ASSERT(python_expect("with-as is __enter__'s value", &file, 1, "CALLS", "main.py",
                             "extract", "Method", 1));
    }
    {
        /* Both defs of extract share a name: the query counts targets per class below. */
        const python_proof_file_t files[] = {
            {"lib.py", archive},
            {"main.py",
             "from lib import Shut\ndef run():\n    with Shut() as held:\n        held.extract()\n"}};
        ASSERT_EQ(python_route_edges(files, 2, "main.py", "CALLS", "lib.py", "extract", "Method",
                                     false),
                  1);
        ASSERT(python_expect("__enter__ returning another class", files, 2, "CALLS", "lib.py",
                             "extract", "Method", 1));
    }
    EXPECT_CALL("with-as with no __enter__ of its own", "main.py", "extract", "Method", 0,
                {"main.py", "from external import Base\nclass Archive(Base):\n"
                            "    def extract(self):\n        return 1\n"
                            "def run():\n    with Archive() as archive:\n        archive.extract()\n"});
    static const char errors[] = "class Invalid(Exception):\n    def update(self):\n        return 1\n"
                                 "class Other(Exception):\n    def update(self):\n        return 2\n";
    {
        char text[512];
        snprintf(text, sizeof(text),
                 "from errors import Invalid, Other\ndef run():\n    try:\n        pass\n"
                 "    except Invalid as e:\n        e.update()\n");
        const python_proof_file_t files[] = {{"errors.py", errors}, {"main.py", text}};
        ASSERT(python_expect("except-as is the class", files, 2, "CALLS", "errors.py", "update",
                             "Method", 1));
    }
    {
        /* base.py: one name across two clauses; each clause sees its own class. */
        const python_proof_file_t files[] = {
            {"errors.py", "class Invalid(Exception):\n    def only(self):\n        return 1\n"
                          "class Other(Exception):\n    def rest(self):\n        return 2\n"},
            {"main.py",
             "from errors import Invalid, Other\ndef run():\n    try:\n        pass\n"
             "    except Invalid as e:\n        e.only()\n    except Other as e:\n        e.rest()\n"}};
        ASSERT(python_expect("first clause", files, 2, "CALLS", "errors.py", "only", "Method", 1));
        ASSERT(python_expect("second clause", files, 2, "CALLS", "errors.py", "rest", "Method", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"errors.py", errors},
            {"main.py", "from errors import Invalid, Other\ndef run():\n    try:\n        pass\n"
                        "    except (Invalid, Other) as e:\n        e.update()\n"}};
        ASSERT(python_expect("a tuple of classes says neither", files, 2, "CALLS", "errors.py",
                             "update", "Method", 0));
    }
    PASS();
}

TEST(resolver_python_the_binder_every_path_passes_last) {
    static const char classes[] = "class A:\n    def go(self):\n        return 1\n"
                                  "    def clone(self):\n        return self\n"
                                  "class B:\n    def go(self):\n        return 2\n";
    static const struct {
        const char *what, *body;
        int a, b;
    } cases[] = {
        {"straight line", "    x = A()\n    x = B()\n    x.go()\n", 0, 1},
        {"before the rebinding", "    x = A()\n    x.go()\n    x = B()\n", 1, 0},
        /* contrib/postgres/constraints.py: the loop variable rebound by a real class. */
        {"rebinding a loop variable",
         "    for x in items:\n        x = A()\n        x.go()\n", 1, 0},
        {"a nested use after the binder", "    x = A()\n    if c:\n        x.go()\n    x = B()\n", 1, 0},
        {"inside an arm", "    x = B()\n    if c:\n        x = A()\n        x.go()\n", 1, 0},
        {"after an arm, either could reach", "    x = B()\n    if c:\n        x = A()\n    x.go()\n",
         0, 0},
        {"the right side reads the earlier binder", "    x = A()\n    x = x.clone()\n    x.go()\n",
         1, 0},
        /* A later binder comes back round through the loop. */
        {"a loop that rebinds later", "    x = A()\n    for i in items:\n        x.go()\n        x = B()\n",
         0, 0},
        {"a loop that rebinds elsewhere", "    x = A()\n    for i in items:\n        x.go()\n    x = B()\n",
         1, 0},
        {"a while that rebinds later", "    x = A()\n    while c:\n        x.go()\n        x = B()\n",
         0, 0},
        {"binder and use in one iteration", "    for i in items:\n        x = A()\n        x.go()\n        x = B()\n",
         1, 0},
        {"another binder form", "    x = A()\n    for x in items:\n        x.go()\n", 0, 0},
        {"a None between", "    x = A()\n    x = None\n    x.go()\n", 0, 0},
        {"something unfollowable between", "    x = A()\n    x = pick()\n    x.go()\n", 0, 0},
        {"a parameter, rebound later", "    x.go()\n    x = A()\n", 0, 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source), "from lib import A, B\ndef run(x, items, c):\n%s",
                 cases[i].body);
        const python_proof_file_t files[] = {{"lib.py", classes}, {"main.py", source}};
        char label[96];
        snprintf(label, sizeof(label), "%s, A", cases[i].what);
        python_route_release(&python_routes[0]);
        python_route_release(&python_routes[1]);
        /* The two classes share go: their nodes differ by start line, so count by class. */
        int a = python_route_edges(files, 2, "main.py", "CALLS", "lib.py", "go", "Method", false);
        ASSERT_EQ(a, cases[i].a + cases[i].b);
        ASSERT(python_expect(label, files, 2, "CALLS", "lib.py", "go", "Method",
                             cases[i].a + cases[i].b));
    }
    /* Which class, for the cases where only one is right. */
    EXPECT_CALL("straight line picks the last", "lib.py", "only_b", "Method", 1,
                {"lib.py", "class A:\n    def only_a(self):\n        return 1\n"
                           "class B:\n    def only_b(self):\n        return 2\n"},
                {"main.py", "from lib import A, B\ndef run():\n    x = A()\n    x = B()\n"
                            "    x.only_b()\n"});
    EXPECT_CALL("and not the one before", "lib.py", "only_a", "Method", 0,
                {"lib.py", "class A:\n    def only_a(self):\n        return 1\n"
                           "class B:\n    def only_b(self):\n        return 2\n"},
                {"main.py", "from lib import A, B\ndef run():\n    x = A()\n    x = B()\n"
                            "    x.only_a()\n"});
    PASS();
}

TEST(resolver_python_names_a_module_states_only_for_checkers) {
    /* celery/__init__.py: lazy at run time, stated under TYPE_CHECKING. */
    static const char canvas[] = "class chain:\n    def link(self):\n        return 1\n";
    static const char user[] = "from pkg import chain\ndef run():\n    s = chain()\n    s.link()\n";
    {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
                                "    from pkg.canvas import chain\n"
                                "def __getattr__(name):\n    return load(name)\n"},
            {"pkg/canvas.py", canvas},
            {"main.py", user}};
        ASSERT(python_expect("the class", files, 3, "CALLS", "pkg/canvas.py", "chain", "Class", 1));
        ASSERT(python_expect("its method", files, 3, "CALLS", "pkg/canvas.py", "link", "Method", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
                                "    from pkg.canvas import chain\nelse:\n    chain = None\n"},
            {"pkg/canvas.py", canvas},
            {"main.py", user}};
        ASSERT(python_expect("a runtime binder competes", files, 3, "CALLS", "pkg/canvas.py",
                             "chain", "Class", 0));
    }
    {
        /* celery: `local.recreate_module(__name__, ...)` builds the module at run time. */
        const python_proof_file_t handed[] = {
            {"pkg/__init__.py", "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
                                "    from pkg.canvas import chain\n"
                                "import local\nlocal.recreate_module(__name__, by={})\n"},
            {"pkg/canvas.py", canvas},
            {"main.py", user}};
        ASSERT(python_expect("a module that hands on its own name", handed, 3, "CALLS",
                             "pkg/canvas.py", "chain", "Class", 1));
        /* Nothing provides the name at run time: no __getattr__, no binder. */
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
                                "    from pkg.canvas import chain\n"},
            {"pkg/canvas.py", canvas},
            {"main.py", user}};
        ASSERT(python_expect("a closed module's checker-only name", files, 3, "CALLS",
                             "pkg/canvas.py", "chain", "Class", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
                                "    from pkg.canvas import *\n"},
            {"pkg/canvas.py", canvas},
            {"main.py", user}};
        ASSERT(python_expect("a closed module's checker-only star", files, 3, "CALLS",
                             "pkg/canvas.py", "chain", "Class", 0));
    }
    {
        /* A lazy package states a whole module's names by a star under TYPE_CHECKING. */
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "import typing\nif typing.TYPE_CHECKING:\n    from .canvas import *\n"
                                "def __getattr__(name):\n    return load(name)\n"},
            {"pkg/canvas.py", "__all__ = ['chain']\nclass chain:\n    def link(self):\n"
                              "        return 1\nclass hidden:\n    pass\n"},
            {"main.py", "from pkg import chain, hidden\ndef run():\n    hidden()\n    s = chain()\n"
                        "    s.link()\n"}};
        ASSERT(python_expect("a star under TYPE_CHECKING", files, 3, "CALLS", "pkg/canvas.py", "link",
                             "Method", 1));
        ASSERT(python_expect("what that star does not export", files, 3, "CALLS", "pkg/canvas.py",
                             "hidden", "Class", 0));
    }
    {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "import os\nif os.environ:\n    from .canvas import *\n"
                                "else:\n    from .other import *\n"},
            {"pkg/canvas.py", "__all__ = ['chain']\nclass chain:\n    pass\n"},
            {"pkg/other.py", "__all__ = ['chain']\nclass chain:\n    pass\n"},
            {"main.py", "from pkg import chain\ndef run():\n    chain()\n"}};
        ASSERT(python_expect("two stars a condition chooses between", files, 4, "CALLS",
                             "pkg/canvas.py", "chain", "Class", 0));
        ASSERT(python_expect("the other of those stars", files, 4, "CALLS", "pkg/other.py", "chain",
                             "Class", 0));
    }
    /* A class body's `X = X` reads X from outside: celery's ResultConsumer. */
    EXPECT_CALL("a class attribute named as what it holds", "main.py", "go", "Method", 1,
                {"main.py", "class Consumer:\n    def go(self):\n        return 1\n"
                            "class Backend:\n    Consumer = Consumer\n    def make(self):\n"
                            "        return self.Consumer()\n"
                            "def run():\n    return Backend().make().go()\n"});
    /* tests/migrations/test_writer.py: exec's assignments go to its third argument. */
    EXPECT_CALL("exec with globals() and its own locals", "main.py", "local", "Function", 1,
                {"main.py", "def local():\n    return 1\ndef safe(code):\n    d = {}\n"
                            "    exec(code, globals(), d)\ndef run():\n    return local()\n"});
    EXPECT_CALL("exec into globals() itself", "main.py", "local", "Function", 0,
                {"main.py", "def local():\n    return 1\ndef safe(code):\n"
                            "    exec(code, globals())\ndef run():\n    return local()\n"});
    /* A computed key names no binding the audit saw. */
    EXPECT_CALL("a computed globals() store", "main.py", "local", "Function", 1,
                {"main.py", "def local():\n    return 1\nglobals()[key()] = None\n"
                            "def run():\n    return local()\n"});
    EXPECT_CALL("a literal globals() store", "main.py", "local", "Function", 0,
                {"main.py", "def local():\n    return 1\nglobals()['local'] = None\n"
                            "def run():\n    return local()\n"});
    {
        /* What the module lacks is unknowable after one, so a submodule is unproven. */
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", "globals()[key()] = None\n"},
            {"pkg/mod.py", "def real():\n    return 1\n"},
            {"main.py", "from pkg import mod\ndef run():\n    return mod.real()\n"}};
        ASSERT(python_expect("absence after a computed store", files, 3, "CALLS", "pkg/mod.py",
                             "real", "Function", 0));
    }
    /* test_geos_mutation.py: a read-only method of the view. */
    EXPECT_CALL("vars().items() reads", "main.py", "local", "Function", 1,
                {"main.py", "def local():\n    return 1\nnames = list(vars().items())\n"
                            "def run():\n    return local()\n"});
    PASS();
}

/* click's @t.overload def prompt, and Command.main. */
TEST(resolver_python_overload_stubs_are_one_declaration) {
    static const char lib[] =
        "import typing as t\nfrom typing import overload\ndef helper():\n    return 1\n"
        "@t.overload\ndef prompt(text: str) -> str: ...\n"
        "@t.overload\ndef prompt(text: int) -> int: ...\n"
        "def prompt(text):\n    return helper()\n"
        "class Command:\n    @overload\n    def main(self, a: str) -> str: ...\n"
        "    @overload\n    def main(self, a: int) -> int: ...\n"
        "    def main(self, a):\n        return self.invoke(a)\n"
        "    def invoke(self, a):\n        return helper()\n";
    static const char user[] = "from lib import prompt, Command\ndef run():\n    prompt('x')\n"
                               "    Command().main('x')\n";
    const python_proof_file_t files[] = {{"lib.py", lib}, {"main.py", user}};
    ASSERT(python_expect("an overloaded function", files, 2, "CALLS", "lib.py", "prompt",
                         "Function", 1));
    ASSERT(python_expect("an overloaded method", files, 2, "CALLS", "lib.py", "main", "Method", 1));
    ASSERT(python_expect_from("from an overloaded function's body", files, 2, "lib.py", "CALLS",
                              "lib.py", "helper", "Function", 2));
    ASSERT(python_expect_from("from an overloaded method's body", files, 2, "lib.py", "CALLS",
                              "lib.py", "invoke", "Method", 1));
    /* A project decorator of that name proves nothing: two defs, the later one rebinds. */
    EXPECT_CALL("a project decorator named overload", "main.py", "go", "Function", 0,
                {"main.py", "from project import overload\n@overload\ndef go():\n    return 1\n"
                            "def go():\n    return 2\ndef run():\n    return go()\n"},
                {"project.py", "def overload(function):\n    return function\n"});
    PASS();
}

TEST(resolver_python_loading_a_child_the_parent_imported) {
    /* django.db.models: `from django.db.models import signals` in its own __init__, and other
     * files load django.db.models.signals. Both name the one module. */
    const python_proof_file_t files[] = {
        {"pkg/__init__.py", "from pkg import signals\n"},
        {"pkg/signals.py", "class Signal:\n    def send(self):\n        return 1\n"
                           "pre_delete = Signal()\n"},
        {"loader.py", "from pkg.signals import pre_delete\n"},
        {"main.py", "from pkg import signals\ndef run():\n    return signals.pre_delete.send()\n"}};
    ASSERT(python_expect("through the parent", files, 4, "CALLS", "pkg/signals.py", "send",
                         "Method", 1));
    /* Under another name the parent's binding is not the module's own attribute. */
    const python_proof_file_t renamed[] = {
        {"pkg/__init__.py", "from pkg.other import value as signals\n"},
        {"pkg/other.py", "value = 1\n"},
        {"pkg/signals.py", "class Signal:\n    def send(self):\n        return 1\n"
                           "pre_delete = Signal()\n"},
        {"loader.py", "from pkg.signals import pre_delete\n"},
        {"main.py", "from pkg import signals\ndef run():\n    return signals.pre_delete.send()\n"}};
    ASSERT(python_expect("a value of that name competes", renamed, 5, "CALLS", "pkg/signals.py",
                         "send", "Method", 0));
    PASS();
}

/* ── What annotations and isinstance say ──────────────────────────────────
 * Forms from click (dict[str, _Option], -> list[Parameter]), httpx (self: T -> T, async with),
 * poetry (isinstance on a loop variable) and django (isinstance on self.rhs). */
TEST(resolver_python_annotated_containers_hold_their_element) {
    static const char lib[] =
        "from __future__ import annotations\nimport typing as t\n"
        "from typing import Iterator, Optional, Self, TypeVar\n"
        "from contextlib import contextmanager\nT = TypeVar('T', bound='Client')\n"
        "class Param:\n    def clean(self):\n        return 1\n"
        "class Option:\n    def process(self):\n        return 1\n"
        "class Response:\n    def read(self):\n        return 1\n"
        "class Gen(t.Generic[T]):\n    def own(self):\n        return 1\n"
        "class Client:\n    def __init__(self):\n        self._opts: dict[str, Option] = {}\n"
        "        self._params: list[Param] = []\n"
        "    def __enter__(self: T) -> T:\n        return self\n"
        "    def chain(self) -> Self:\n        return self\n"
        "    def only(self):\n        return 1\n"
        "    def params(self) -> list[Param]:\n        return self._params\n"
        "    def first(self) -> Optional[Param]:\n        return None\n"
        "    @contextmanager\n    def stream(self) -> Iterator[Response]:\n"
        "        yield Response()\n"
        "class Sub(Client):\n    def only(self):\n        return 2\n";
    static const struct {
        const char *what, *body, *name;
        int expected;
    } cases[] = {
        {"for over a returned list",
         "def run(c: Client):\n    for p in c.params():\n        p.clean()\n", "clean", 1},
        {"for over an annotated attribute",
         "def run(c: Client):\n    for p in c._params:\n        p.clean()\n", "clean", 1},
        {"a comprehension", "def run(c: Client):\n    return [p.clean() for p in c._params]\n",
         "clean", 1},
        {"sorted keeps what it was given",
         "def run(c: Client):\n    for p in sorted(c._params, key=None):\n        p.clean()\n",
         "clean", 1},
        {"reversed keeps what it was given",
         "def run(c: Client):\n    for p in reversed(c._params):\n        p.clean()\n", "clean", 1},
        {"a project function named sorted says nothing",
         "from other import sorted\ndef run(c: Client):\n    for p in sorted(c._params):\n"
         "        p.clean()\n",
         "clean", 0},
        {"a subscript of a list annotation",
         "def run(items: list[Param]):\n    return items[0].clean()\n", "clean", 1},
        {"a slice stays a list",
         "def run(items: list[Param]):\n    for p in items[1:]:\n        p.clean()\n", "clean", 1},
        {"a dict value by key", "def run(c: Client, k):\n    c._opts[k].process()\n", "process", 1},
        {"a dict value by get", "def run(c: Client, k):\n    c._opts.get(k).process()\n", "process",
         1},
        {"dict values", "def run(c: Client):\n    for o in c._opts.values():\n        o.process()\n",
         "process", 1},
        {"iterating a dict yields keys",
         "def run(c: Client):\n    for o in c._opts:\n        o.process()\n", "process", 0},
        {"items, the value",
         "def run(c: Client):\n    for k, o in c._opts.items():\n        o.process()\n", "process",
         1},
        {"items of a class that has its own",
         "class Mine:\n    def items(self):\n        return [(1, 2)]\n"
         "    def values(self) -> list[Param]:\n        return []\n"
         "def run(m: Mine):\n    for k, p in m.items():\n        p.clean()\n",
         "clean", 0},
        {"items, the key says nothing",
         "def run(c: Client):\n    for o, k in c._opts.items():\n        o.process()\n", "process",
         0},
        {"enumerate, the element",
         "def run(c: Client):\n    for i, p in enumerate(c._params):\n        p.clean()\n",
         "clean", 1},
        {"enumerate, the count says nothing",
         "def run(c: Client):\n    for p, i in enumerate(c._params):\n        p.clean()\n",
         "clean", 0},
        {"zip, by position",
         "def run(c: Client, xs):\n    for x, p in zip(xs, c._params):\n        p.clean()\n",
         "clean", 1},
        {"zip, the other position",
         "def run(c: Client, xs):\n    for p, x in zip(xs, c._params):\n        p.clean()\n",
         "clean", 0},
        {"a project enumerate says nothing",
         "from other import enumerate\ndef run(c: Client):\n"
         "    for i, p in enumerate(c._params):\n        p.clean()\n",
         "clean", 0},
        {"a walrus", "def run(c: Client):\n    if (p := c.first()):\n        p.clean()\n", "clean",
         1},
        {"a typing alias",
         "import typing as t\ndef run(items: t.Sequence[Param]):\n    for p in items:\n"
         "        p.clean()\n",
         "clean", 1},
        {"typing.cast", "import typing as t\ndef run(v):\n    t.cast('Param', v).clean()\n", "clean",
         1},
        {"a project cast is not typing's",
         "from other import cast\ndef run(v):\n    cast('Param', v).clean()\n", "clean", 0},
        {"type[X] is the class",
         "def run(kind: type[Param]):\n    return kind().clean()\n", "clean", 1},
        {"a generic of the project's own",
         "def run(g: Gen[int]):\n    return g.own()\n", "own", 1},
        {"a contextmanager's yielded type",
         "def run(c: Client):\n    with c.stream() as r:\n        r.read()\n", "read", 1},
        /* Nothing here says which class holds the name. */
        {"an unannotated parameter", "def run(items):\n    for p in items:\n        p.clean()\n",
         "clean", 0},
        {"nested containers are not carried",
         "def run(items: list[list[Param]]):\n    for p in items:\n        p.clean()\n", "clean", 0},
        {"a tuple of two types is not a sequence of one",
         "def run(items: tuple[Param, Option]):\n    return items[0].clean()\n", "clean", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[640];
        snprintf(source, sizeof(source), "from lib import Client, Param, Option, Gen\n%s",
                 cases[i].body);
        const python_proof_file_t files[] = {{"lib.py", lib},
                                             {"other.py", "def sorted(x):\n    return 1\n"
                                                          "def cast(x, y):\n    return 1\n"
                                                          "def enumerate(x):\n    return 1\n"},
                                             {"main.py", source}};
        ASSERT(python_expect(cases[i].what, files, 3, "CALLS", "lib.py", cases[i].name, "Method",
                             cases[i].expected));
    }
    /* A method annotated to return its receiver gives the receiver's class. */
    {
        const python_proof_file_t files[] = {
            {"lib.py", lib},
            {"main.py", "from lib import Sub\ndef run(c: Sub):\n    c.chain().only()\n"
                        "    with c as held:\n        held.only()\n"}};
        int found = python_route_edges(files, 2, "main.py", "CALLS", "lib.py", "only", "Method",
                                       false);
        ASSERT_EQ(found, 1);
    }
    PASS();
}

TEST(resolver_python_isinstance_narrows_where_it_holds) {
    static const char classes[] = "class Stmt:\n    def rename(self):\n        return 1\n"
                                  "class Other:\n    def rename(self):\n        return 2\n"
                                  "def make():\n    return None\n";
    static const struct {
        const char *what, *body;
        int expected;
    } cases[] = {
        {"a parameter", "def run(x):\n    if isinstance(x, Stmt):\n        x.rename()\n", 1},
        {"a loop variable",
         "def run(items):\n    for x in items:\n        if isinstance(x, Stmt):\n"
         "            x.rename()\n",
         1},
        {"self's attribute",
         "class H:\n    def run(self):\n        if isinstance(self.rhs, Stmt):\n"
         "            self.rhs.rename()\n",
         1},
        {"an early exit", "def run(x):\n    if not isinstance(x, Stmt):\n        return None\n"
                          "    x.rename()\n", 1},
        {"an early raise", "def run(x):\n    if not isinstance(x, Stmt):\n        raise ValueError()\n"
                           "    x.rename()\n", 1},
        {"an assert", "def run(x):\n    assert isinstance(x, Stmt)\n    x.rename()\n", 1},
        {"a conjunct", "def run(x, c):\n    if c and isinstance(x, Stmt):\n        x.rename()\n", 1},
        {"after the other conjunct",
         "def run(x, c):\n    if isinstance(x, Stmt) and x.rename():\n        pass\n", 1},
        {"elif", "def run(x):\n    if x:\n        pass\n    elif isinstance(x, Stmt):\n"
                 "        x.rename()\n", 1},
        /* Where it does not hold. */
        {"after the if", "def run(x):\n    if isinstance(x, Stmt):\n        pass\n    x.rename()\n",
         0},
        {"the else", "def run(x):\n    if isinstance(x, Stmt):\n        pass\n    else:\n"
                     "        x.rename()\n", 0},
        {"a not without the exit",
         "def run(x):\n    if not isinstance(x, Stmt):\n        print(x)\n    x.rename()\n", 0},
        {"a tuple of classes",
         "def run(x):\n    if isinstance(x, (Stmt, Other)):\n        x.rename()\n", 0},
        {"rebound inside", "def run(x):\n    if isinstance(x, Stmt):\n        x = make()\n"
                           "        x.rename()\n", 0},
        {"stored inside",
         "class H:\n    def run(self):\n        if isinstance(self.rhs, Stmt):\n"
         "            self.rhs = make()\n            self.rhs.rename()\n",
         0},
        {"in a nested function",
         "def run(x):\n    if isinstance(x, Stmt):\n        def later():\n            x.rename()\n",
         0},
        {"a shadowed isinstance",
         "def run(x, isinstance):\n    if isinstance(x, Stmt):\n        x.rename()\n", 0},
        {"a computed value",
         "def run(x):\n    if isinstance(x.pop(), Stmt):\n        x.pop().rename()\n", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source), "from lib import Stmt, Other, make\n%s", cases[i].body);
        const python_proof_file_t files[] = {{"lib.py", classes}, {"main.py", source}};
        ASSERT(python_expect(cases[i].what, files, 2, "CALLS", "lib.py", "rename", "Method",
                             cases[i].expected));
    }
    PASS();
}

/* ── The smaller shapes django's tree has ───────────────────────────────── */
TEST(resolver_python_class_and_module_shapes) {
    /* A class stated inside a class, read through either. */
    EXPECT_CALL("a nested class by its owner", "main.py", "go", "Method", 1,
                {"main.py", "class Tests:\n    class Form:\n        def go(self):\n"
                            "            return 1\n"
                            "    def run(self):\n        form = Tests.Form()\n"
                            "        return form.go()\n"});
    EXPECT_CALL("a nested class by self", "main.py", "go", "Method", 1,
                {"main.py", "class Tests:\n    class Form:\n        def go(self):\n"
                            "            return 1\n"
                            "    def run(self):\n        return self.Form().go()\n"});
    /* geos: ListMixin stores self._get_single_internal only when no subclass declares it. */
    EXPECT_CALL("a store in a base beyond the declaring class", "main.py", "go", "Method", 1,
                {"main.py", "class Mixin:\n    def __init__(self):\n        self.go = make()\n"
                            "class Child(Mixin):\n    def go(self):\n        return 1\n"
                            "    def run(self):\n        return self.go()\n"});
    EXPECT_CALL("a store in the declaring class", "main.py", "go", "Method", 0,
                {"main.py", "class Child:\n    def __init__(self):\n        self.go = make()\n"
                            "    def go(self):\n        return 1\n"
                            "    def run(self):\n        return self.go()\n"});
    /* point.py: a property's setter is the same property. */
    {
        const python_proof_file_t files[] = {
            {"main.py", "def helper():\n    return 1\nclass P:\n    @property\n    def x(self):\n"
                        "        return helper()\n    @x.setter\n    def x(self, value):\n"
                        "        helper()\n"}};
        ASSERT(python_expect_from("calls from a property with a setter", files, 1, "main.py", "CALLS",
                                  "main.py", "helper", "Function", 1));
    }
    /* tests/backends: a subclass's store selects over its base's, as a method would. */
    {
        const python_proof_file_t files[] = {
            {"lib.py", "class Table:\n    def only_table(self):\n        return 1\n"
                       "class Columns:\n    def only_columns(self):\n        return 2\n"},
            {"main.py", "from lib import Table, Columns\nclass Base:\n    def setUp(self):\n"
                        "        self.reference = Table()\n"
                        "class Sub(Base):\n    def setUp(self):\n"
                        "        self.reference = Columns()\n"
                        "    def run(self):\n        return self.reference.only_columns()\n"}};
        ASSERT(python_expect("the nearer store selects", files, 2, "CALLS", "lib.py", "only_columns",
                             "Method", 1));
    }
    /* click: self.type: ParamType = ... in Parameter, self.type = convert(...) in Option. */
    EXPECT_CALL("an annotation holds what a subclass stores", "main.py", "go", "Method", 1,
                {"main.py", "class Kind:\n    def go(self):\n        return 1\n"
                            "class Base:\n    def __init__(self):\n"
                            "        self.kind: Kind = make()\n"
                            "class Sub(Base):\n    def set(self):\n        self.kind = other()\n"
                            "    def run(self):\n        return self.kind.go()\n"});
    /* tests: cls.a = Article() in setUpTestData, read back as cls.a. */
    EXPECT_CALL("a store through cls read through cls", "main.py", "go", "Method", 1,
                {"main.py", "class Article:\n    def go(self):\n        return 1\n"
                            "class T:\n    @classmethod\n    def setup(cls):\n"
                            "        cls.a = Article()\n        cls.a.go()\n"});
    /* `self.go = make()` rebinds an instance's name, and the class keeps its own. */
    EXPECT_CALL("a declared method through its class beside an instance store", "main.py", "go",
                "Method", 1,
                {"main.py", "class T:\n    def __init__(self):\n        self.go = make()\n"
                            "    def go(self):\n        return 1\n"
                            "def run(t):\n    return T.go(t)\n"});
    EXPECT_CALL("a store through cls over the class's None", "main.py", "go", "Method", 1,
                {"main.py", "class Article:\n    def go(self):\n        return 1\n"
                            "class T:\n    a = None\n    @classmethod\n    def setup(cls):\n"
                            "        cls.a = Article()\n        cls.a.go()\n"});
    EXPECT_CALL("a store through cls beside a store of something else", "main.py", "go", "Method",
                0,
                {"main.py", "class Article:\n    def go(self):\n        return 1\n"
                            "class T:\n    @classmethod\n    def setup(cls, other):\n"
                            "        cls.a = Article()\n        cls.a = other\n"
                            "        cls.a.go()\n"});
    /* `app.conf = Mock()` says what app.conf holds from there on. */
    EXPECT_CALL("a store through a receiver before the read", "main.py", "go", "Method", 0,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class T:\n    x = A()\n"
                            "def run(t: T):\n    t.x = make()\n    t.x.go()\n"});
    EXPECT_CALL("a store of what it was declared to hold", "main.py", "go", "Method", 1,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class T:\n    x = A()\n"
                            "def run(t: T):\n    t.x = A()\n    t.x.go()\n"});
    EXPECT_CALL("a store of something else", "main.py", "go", "Method", 0,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class B:\n    def go(self):\n        return 2\n"
                            "class T:\n    x = A()\n"
                            "def run(t: T):\n    t.x = B()\n    t.x.go()\n"});
    {
        /* The member itself is still the member: `r.content = x` then reading r.content. */
        const python_proof_file_t files[] = {
            {"main.py", "class R:\n    @property\n    def content(self):\n        return 1\n"
                        "    @content.setter\n    def content(self, value):\n        pass\n"
                        "def run(r: R):\n    r.content = 2\n    return r.content\n"}};
        ASSERT(python_expect("a read of the member just stored", files, 1, "USAGE", "main.py",
                             "content", "Method", 1));
    }
    /* A class-body value whose class has __get__: the read runs that. */
    EXPECT_CALL("a descriptor's own method", "main.py", "go", "Method", 0,
                {"main.py", "class D:\n    def __get__(self, i, o):\n        return make()\n"
                            "    def go(self):\n        return 1\n"
                            "class T:\n    x = D()\n"
                            "    def run(self):\n        self.x.go()\n"});
    EXPECT_CALL("what a descriptor's __get__ returns", "main.py", "go", "Method", 1,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class D:\n    def __get__(self, i, o):\n        return A()\n"
                            "class T:\n    x = D()\n"
                            "    def run(self):\n        self.x.go()\n"});
    EXPECT_CALL("a store through a receiver after the read", "main.py", "go", "Method", 1,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class T:\n    x = A()\n"
                            "def run(t: T):\n    t.x.go()\n    t.x = make()\n"});
    EXPECT_CALL("a store through that spelling in another function", "main.py", "go", "Method", 1,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class T:\n    x = A()\n"
                            "def other(t: T):\n    t.x = make()\n"
                            "def run(t: T):\n    t.x.go()\n"});
    EXPECT_CALL("a store at module level before the function", "main.py", "go", "Method", 0,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class T:\n    x = A()\n"
                            "t = T()\nt.x = make()\n"
                            "def run():\n    t.x.go()\n"});
    EXPECT_CALL("a store past the first attribute of self", "main.py", "go", "Method", 0,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class H:\n    y = A()\n"
                            "class T:\n    def __init__(self):\n        self.x = H()\n"
                            "    def run(self):\n        self.x.y = make()\n"
                            "        self.x.y.go()\n"});
    EXPECT_CALL("the value of a store reads what was there", "main.py", "go", "Method", 1,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "class T:\n    x = A()\n"
                            "def run(t: T):\n    t.x = t.x.go()\n"});
    /* QuerySet.as_manager: `as_manager = classmethod(as_manager)`, the decorator spelled out. */
    EXPECT_CALL("a classmethod spelled out", "main.py", "go", "Method", 1,
                {"main.py", "class T:\n    def go(cls):\n        return 1\n"
                            "    go = classmethod(go)\n"
                            "class S(T):\n    pass\n"
                            "def run():\n    return S.go()\n"});
    EXPECT_CALL("a staticmethod spelled out", "main.py", "go", "Method", 1,
                {"main.py", "class T:\n    def go():\n        return 1\n"
                            "    go = staticmethod(go)\n"
                            "    def run(self):\n        return self.go()\n"});
    EXPECT_CALL("a project's own classmethod", "main.py", "go", "Method", 0,
                {"main.py", "from other import classmethod\n"
                            "class T:\n    def go(cls):\n        return 1\n"
                            "    go = classmethod(go)\n"
                            "def run():\n    return T.go()\n"},
                {"other.py", "def classmethod(f):\n    return f\n"});
    EXPECT_CALL("a classmethod of something else", "main.py", "go", "Method", 0,
                {"main.py", "def other(cls):\n    return 1\n"
                            "class T:\n    def go(cls):\n        return 1\n"
                            "    go = classmethod(other)\n"
                            "def run():\n    return T.go()\n"});
    /* `def f(self, Request=Request)`: what the signature says it holds. */
    EXPECT_CALL("a parameter defaulting to a class", "main.py", "go", "Method", 1,
                {"main.py", "class Request:\n    def go(self):\n        return 1\n"
                            "def run(Request=Request):\n    return Request().go()\n"});
    EXPECT_CALL("a parameter defaulting to an instance", "main.py", "go", "Method", 1,
                {"main.py", "class Apps:\n    def go(self):\n        return 1\nglobal_apps = Apps()\n"
                            "def run(apps=global_apps):\n    return apps.go()\n"});
    EXPECT_CALL("a parameter with no default says nothing", "main.py", "go", "Method", 0,
                {"main.py", "class Request:\n    def go(self):\n        return 1\n"
                            "def run(Request):\n    return Request().go()\n"});
    /* `questioner or Questioner()` for a parameter defaulting to None. */
    EXPECT_CALL("None or a class", "main.py", "go", "Method", 1,
                {"main.py", "class Q:\n    def go(self):\n        return 1\n"
                            "class A:\n    def __init__(self, q=None):\n"
                            "        self.q = q or Q()\n"
                            "    def run(self):\n        return self.q.go()\n"});
    EXPECT_CALL("a required parameter or a class", "main.py", "go", "Method", 0,
                {"main.py", "class Q:\n    def go(self):\n        return 1\n"
                            "class A:\n    def __init__(self, q):\n"
                            "        self.q = q or Q()\n"
                            "    def run(self):\n        return self.q.go()\n"});
    /* `A() if c else None`. */
    EXPECT_CALL("a value or None", "main.py", "go", "Method", 1,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "def run(c):\n    x = A() if c else None\n    return x.go()\n"});
    EXPECT_CALL("two values", "main.py", "go", "Method", 0,
                {"main.py", "class A:\n    def go(self):\n        return 1\n"
                            "def run(c, y):\n    x = A() if c else y\n    return x.go()\n"});
    /* django.contrib.gis.db.models: a star of a module whose __all__ is computed. */
    {
        const python_proof_file_t files[] = {
            {"base/__init__.py", "from base.parts import __all__ as parts_all\n"
                                 "from base.parts import *\n"
                                 "__all__ = parts_all\n__all__ += ['Q']\nclass Q:\n    pass\n"},
            {"base/parts.py", "__all__ = ['Count']\nclass Count:\n    pass\n"},
            {"gis/__init__.py", "from base import *\n"},
            {"gis/functions.py", "class Area:\n    pass\n"},
            {"main.py", "from gis import functions, Q, Count\ndef run():\n    functions.Area()\n"
                        "    Q()\n    Count()\n"}};
        ASSERT(python_expect("a submodule past that star", files, 5, "CALLS", "gis/functions.py",
                             "Area", "Class", 1));
        ASSERT(python_expect("a public name through that star", files, 5, "CALLS", "base/__init__.py",
                             "Q", "Class", 1));
        ASSERT(python_expect("a name that star itself got by a star", files, 5, "CALLS",
                             "base/parts.py", "Count", "Class", 1));
    }
    {
        /* A dynamic module stays open whatever its __all__ says. */
        const python_proof_file_t files[] = {
            {"base/__init__.py", "__all__ = ['Q']\ndef __getattr__(name):\n    return 1\n"},
            {"gis/__init__.py", "from base import *\n"},
            {"gis/functions.py", "class Area:\n    pass\n"},
            {"main.py", "from gis import functions\ndef run():\n    functions.Area()\n"}};
        ASSERT(python_expect("a star of a module with __getattr__", files, 4, "CALLS",
                             "gis/functions.py", "Area", "Class", 0));
    }
    /* views/decorators: if/else twins of a nested def are one declaration. */
    {
        const python_proof_file_t files[] = {
            {"main.py",
             "from functools import wraps\ndef helper():\n    return 1\n"
             "def deco(func, c):\n    if c:\n        async def inner(request):\n"
             "            helper()\n    else:\n        def inner(request):\n"
             "            helper()\n    return wraps(func)(inner)\n"}};
        ASSERT(python_expect_from("calls from nested twin defs", files, 1, "main.py", "CALLS",
                                  "main.py", "helper", "Function", 1));
    }
    {
        const python_proof_file_t files[] = {
            {"main.py",
             "def helper():\n    return 1\ndef deco(c):\n    def inner():\n        helper()\n"
             "    def inner():\n        return 2\n    return inner\n"}};
        ASSERT(python_expect_from("two defs in a row are a rebinding", files, 1, "main.py", "CALLS",
                                  "main.py", "helper", "Function", 0));
    }
    PASS();
}

SUITE(resolver_python) {
    RUN_TEST(resolver_python_ordinary_syntax_keeps_the_file);
    RUN_TEST(resolver_python_only_real_namespace_effects_are_dynamic);
    RUN_TEST(resolver_python_member_spelling_is_not_a_lexical_read);
    RUN_TEST(resolver_python_function_body_sees_later_module_binders);
    RUN_TEST(resolver_python_conditional_definitions_are_honest);
    RUN_TEST(resolver_python_import_forms_reach_the_declaration);
    RUN_TEST(resolver_python_absolute_imports_follow_the_search_path);
    RUN_TEST(resolver_python_submodule_import_survives_a_working_package);
    RUN_TEST(resolver_python_star_import_follows_the_module_interface);
    RUN_TEST(resolver_python_self_selects_through_c3_order);
    python_route_release(&python_routes[0]);
    python_route_release(&python_routes[1]);
}

SUITE(resolver_python_members) {
    RUN_TEST(resolver_python_super_and_class_level_receivers);
    RUN_TEST(resolver_python_receiver_types_need_a_fact);
    RUN_TEST(resolver_python_class_body_values_can_be_replaced);
    RUN_TEST(resolver_python_class_comprehension_skips_the_class);
    RUN_TEST(resolver_python_shadowed_super_and_type_are_not_the_builtins);
    python_route_release(&python_routes[0]);
    python_route_release(&python_routes[1]);
}

SUITE(resolver_python_layout) {
    RUN_TEST(resolver_python_stated_source_root_reaches_the_module);
    RUN_TEST(resolver_python_namespace_package_under_a_source_root);
    RUN_TEST(resolver_python_manifests_state_their_roots);
    RUN_TEST(resolver_python_source_root_decoys_abstain);
    RUN_TEST(resolver_python_workspace_members_see_each_other);
    RUN_TEST(resolver_python_only_a_store_of_that_name_blocks_a_submodule);
    RUN_TEST(resolver_python_writes_through_the_submodule_leave_its_identity);
    RUN_TEST(resolver_python_module_getattr_only_answers_what_is_missing);
    RUN_TEST(resolver_python_package_importing_its_own_child);
    RUN_TEST(resolver_python_names_a_module_states_only_for_checkers);
    RUN_TEST(resolver_python_overload_stubs_are_one_declaration);
    RUN_TEST(resolver_python_loading_a_child_the_parent_imported);
    RUN_TEST(resolver_python_package_init_hands_its_namespace_on);
    python_route_release(&python_routes[0]);
    python_route_release(&python_routes[1]);
}

SUITE(resolver_python_flow) {
    RUN_TEST(resolver_python_special_methods_that_read_through_the_base);
    RUN_TEST(resolver_python_returns_follow_the_receiver);
    RUN_TEST(resolver_python_with_and_except_bind_what_they_say);
    RUN_TEST(resolver_python_the_binder_every_path_passes_last);
    RUN_TEST(resolver_python_annotated_containers_hold_their_element);
    RUN_TEST(resolver_python_isinstance_narrows_where_it_holds);
    python_route_release(&python_routes[0]);
    python_route_release(&python_routes[1]);
}

SUITE(resolver_python_shapes) {
    RUN_TEST(resolver_python_class_and_module_shapes);
    RUN_TEST(resolver_python_rebound_receivers_abstain);
    RUN_TEST(resolver_python_module_level_instance_has_its_class);
    RUN_TEST(resolver_python_decorators_keep_identity_only_when_proven);
    RUN_TEST(resolver_python_typed_receiver_preserves_crossfile_method);
    RUN_TEST(resolver_python_reexport_chain_preserves_actual_declaration);
    RUN_TEST(resolver_python_imported_class_is_the_static_callable);
    RUN_TEST(resolver_python_reexport_shadow_and_cycle_abstain);
    RUN_TEST(resolver_python_package_value_blocks_physical_submodule);
    python_route_release(&python_routes[0]);
    python_route_release(&python_routes[1]);
}
