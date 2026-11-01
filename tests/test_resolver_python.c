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

static int python_route_edges(const python_proof_file_t *files, size_t count, const char *relation,
                              const char *target, const char *name, const char *label,
                              bool parallel) {
    const char *root = th_mktempdir("cbm_python_rule");
    if (!root)
        return -1;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
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
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (store && sqlite3_prepare_v2(cbm_store_get_db(store),
                                    "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                                    "JOIN nodes t ON t.id=e.target_id WHERE e.type=?4 "
                                    "AND s.file_path='main.py' AND t.file_path=?1 AND t.name=?2 "
                                    "AND t.label=?3",
                                    -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, target, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, name, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 3, label, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 4, relation, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            edges = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return edges;
}

/* Both drivers must agree on the expected count. */
static bool python_expect(const char *what, const python_proof_file_t *files, size_t count,
                          const char *relation, const char *target, const char *name,
                          const char *label, int expected) {
    for (int parallel = 0; parallel < 2; parallel++) {
        int found = python_route_edges(files, count, relation, target, name, label, parallel != 0);
        if (found != expected) {
            fprintf(stderr, "python rule [%s] parallel=%d %s -> %s:%s found=%d expected=%d\n", what,
                    parallel, relation, target, name, found, expected);
            return false;
        }
    }
    return true;
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
        "def merged():\n    return {**globals(), 'x': 1}\n"};
    for (size_t i = 0; i < sizeof(inert) / sizeof(inert[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "def local():\n    return 1\n%sdef run():\n    return local()\n", inert[i]);
        const python_proof_file_t file = {"main.py", source};
        ASSERT(python_expect(inert[i], &file, 1, "CALLS", "main.py", "local", "Function", 1));
    }
    static const char *effects[] = {"exec('local = None')\n", "eval('1')\n",
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
                 {"main.py", "def helper():\n    return 1\ndef run():\n    kind = helper\n    return kind\n"});
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
    /* A module object handed to unknown code can be written through, and so can
     * a member path that stores. */
    static const char *patchers[] = {"import pkg\ndef patch():\n    install(pkg)\n",
                                     "import pkg\npkg.mod = None\n"};
    for (size_t i = 0; i < sizeof(patchers) / sizeof(patchers[0]); i++) {
        const python_proof_file_t files[] = {
            {"pkg/__init__.py", ""},
            child,
            {"patcher.py", patchers[i]},
            {"main.py", "from pkg import mod\ndef run():\n    return mod.real()\n"}};
        ASSERT(python_expect(patchers[i], files, 4, "CALLS", "pkg/mod.py", "real", "Function", 0));
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
        ASSERT(python_expect("single chain keeps its order", files, 2, "CALLS", "main.py", "mid", "Method", 1));
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
        {"local rebound to another class",
         "from lib.store import Store, Other\ndef run():\n    s = Store()\n    s = Other()\n"
         "    return s.get()\n",
         0},
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
        const python_proof_file_t files[] = {{"main.py", cases[i].source}, store};
        ASSERT(python_expect(cases[i].what, files, 2, "CALLS", "lib/store.py", "get", "Method",
                             cases[i].store_get));
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
        {"@either(path='p')", 1}, {"@replace", 0},     {"@nothing", 0},        {"@opaque", 0},
        {"@swallow", 0},       {"@conditional", 0},    {"@generated", 0},      {"@tagged", 0},
        {"@same('x')", 0},     {"@external_deco", 0},  {"@same\n@replace", 0}, {"@same\n@logged", 1},
        {"@wraps_called", 1},  {"@updates", 1},        {"@update_returned", 1},
        {"@hands_on", 1},      {"@hands_on_direct", 1}, {"@hands_to_bad", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[768];
        snprintf(source, sizeof(source),
                 "from lib.deco import same, logged, tagged, either, replace, nothing, opaque, "
                 "swallow, conditional, generated, wraps_called, updates, update_returned, "
                 "hands_on, hands_on_direct, hands_to_bad\nfrom external import external_deco\n"
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
    EXPECT_CALL("base behind an unproven decorator", "main.py", "save", "Method", 0,
                {"main.py", "from external import wrap\n@wrap\nclass Base:\n"
                            "    def save(self):\n        return 1\n"
                            "class Child(Base):\n    def run(self):\n        return super().save()\n"});
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
                             "    @value.setter\n    def value(self, item):\n        pass\n"
                             "    def read(self):\n        return self.value\n";
        const python_proof_file_t files[] = {{"main.py", source}};
        ASSERT(python_expect("property read", files, 1, "USAGE", "main.py", "value", "Method", 1));
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
    /* Unknown decorators on a method stop its lookup too. */
    EXPECT_CALL("unknown method decorator", "main.py", "go", "Method", 0,
                {"main.py", "from external import wrap\nclass A:\n    @wrap\n    def go(self):\n"
                            "        return 1\n    def run(self):\n        return self.go()\n"});
    PASS();
}

SUITE(resolver_python) {
    RUN_TEST(resolver_python_ordinary_syntax_keeps_the_file);
    RUN_TEST(resolver_python_only_real_namespace_effects_are_dynamic);
    RUN_TEST(resolver_python_member_spelling_is_not_a_lexical_read);
    RUN_TEST(resolver_python_function_body_sees_later_module_binders);
    RUN_TEST(resolver_python_conditional_definitions_are_honest);
    RUN_TEST(resolver_python_import_forms_reach_the_declaration);
    RUN_TEST(resolver_python_submodule_import_survives_a_working_package);
    RUN_TEST(resolver_python_star_import_follows_the_module_interface);
    RUN_TEST(resolver_python_self_selects_through_c3_order);
    RUN_TEST(resolver_python_super_and_class_level_receivers);
    RUN_TEST(resolver_python_receiver_types_need_a_fact);
    RUN_TEST(resolver_python_module_level_instance_has_its_class);
    RUN_TEST(resolver_python_decorators_keep_identity_only_when_proven);
    RUN_TEST(resolver_python_typed_receiver_preserves_crossfile_method);
    RUN_TEST(resolver_python_reexport_chain_preserves_actual_declaration);
    RUN_TEST(resolver_python_imported_class_is_the_static_callable);
    RUN_TEST(resolver_python_reexport_shadow_and_cycle_abstain);
    RUN_TEST(resolver_python_package_value_blocks_physical_submodule);
}
