/*
 * py_stdlib_compat.c — Hand-written stdlib knowledge the generated table lacks.
 *
 * Two concerns, one registration function (py_compat_stdlib_register), called
 * at ALL THREE registry-construction sites right after
 * cbm_python_stdlib_register (per-file cbm_run_py_lsp, tier-1
 * cbm_run_py_lsp_cross, tier-2 cbm_py_build_cross_registry):
 *
 *   1. Python 2 dialect layer (py2-stdlib-builtins-compat). The vendored
 *      tree-sitter-python parses py2 source with zero ERROR nodes
 *      (probe-verified: print statement, chevron print, `except E, e:`,
 *      exec, backticks, 0777/123L, `<>`, tuple params), so py2 support is
 *      purely missing KNOWLEDGE: builtins (xrange/unicode/basestring/long/
 *      raw_input/cmp/execfile/unichr/reduce/...), dict iter-methods
 *      (iteritems/iterkeys/itervalues/has_key), and the renamed-module map
 *      (urllib2 -> urllib.request, ConfigParser -> configparser, ...) which
 *      py_bind_import_index consults via py_py2_rewrite_module_qn below.
 *
 *   2. 3.11-3.13 stdlib entries missing from the generated table
 *      (py-stdlib-allowlist-refresh, hand-added because no typeshed checkout
 *      is available to regenerate against): tomllib, configparser, zoneinfo,
 *      asyncio.TaskGroup, io.StringIO / io.BytesIO, socketserver. Entries
 *      follow the generated file's idiom (memset + cbm_registry_add_*) so a
 *      future regeneration with a grown allowlist can delete them wholesale.
 *
 * Static tables only — O(1) added work per registration site, arena-copied
 * nothing (all strings are static). Self-contained: #included from py_lsp.c
 * only (CGo amalgamation pattern, see py_builtins.c). Not a standalone
 * translation unit.
 */

/* One row of the py2 -> py3 renamed-module table. `sentinel_qn` names a row
 * the py3 twin is known to register; the alias is applied only when the
 * sentinel is present in the registry, so the rewrite can never fabricate
 * modules the resolver has no knowledge of (and a project-local module that
 * merely shares the py2 name keeps its project-prefixed QN and never
 * matches the bare `py2` spelling here). */
typedef struct {
    const char *py2;         /* bare py2 module name, e.g. "urllib2"     */
    const char *py3;         /* py3 twin, e.g. "urllib.request"          */
    const char *sentinel_qn; /* registered row proving the twin exists    */
    bool sentinel_is_type;   /* lookup as type (else as func)             */
} PyPy2ModuleTwin;

static const PyPy2ModuleTwin kPy2ModuleTwins[] = {
    {"urllib2", "urllib.request", "urllib.request.urlopen", false},
    {"ConfigParser", "configparser", "configparser.ConfigParser", true},
    {"StringIO", "io", "io.StringIO", true},
    {"cStringIO", "io", "io.StringIO", true},
    {"Queue", "queue", "queue.Queue", true},
    {"httplib", "http.client", "http.client.HTTPResponse", true},
    {"cPickle", "pickle", "pickle.Pickler", true},
    {"SocketServer", "socketserver", "socketserver.TCPServer", true},
    {"urlparse", "urllib.parse", "urllib.parse.ParseResult", true},
    {NULL, NULL, NULL, false},
};

/* Map a py2 module QN (or dotted QN whose first segment is a py2 module) to
 * its py3 twin. Returns the rewritten QN (arena) or NULL when no rewrite
 * applies. Exact-segment match only: "urllib2" and "urllib2.urlopen" match,
 * "myurllib2" and "proj.urllib2" never do. */
static const char *py_py2_rewrite_module_qn(const CBMTypeRegistry *reg, CBMArena *arena,
                                            const char *qn) {
    if (!reg || !arena || !qn || !qn[0])
        return NULL;
    const char *dot = strchr(qn, '.');
    size_t seg_len = dot ? (size_t)(dot - qn) : strlen(qn);
    for (int i = 0; kPy2ModuleTwins[i].py2; i++) {
        const PyPy2ModuleTwin *tw = &kPy2ModuleTwins[i];
        if (strlen(tw->py2) != seg_len || strncmp(qn, tw->py2, seg_len) != 0)
            continue;
        /* Twin-exists gate: alias only toward knowledge that is actually
         * registered, so the binding stays honest when the allowlist shrinks. */
        bool twin_known = tw->sentinel_is_type
                              ? cbm_registry_lookup_type(reg, tw->sentinel_qn) != NULL
                              : cbm_registry_lookup_func(reg, tw->sentinel_qn) != NULL;
        if (!twin_known)
            return NULL;
        if (!dot)
            return tw->py3;
        return cbm_arena_sprintf(arena, "%s%s", tw->py3, dot);
    }
    return NULL;
}

/* ── Registration tables (generated-file idiom, hand-rolled data) ── */

typedef struct {
    const char *qn;
    const char *short_name;
    /* Aliased/base type QN or NULL. py_lookup_attribute_depth follows
     * alias_of, so this both models true aliases (unicode -> str) and gives
     * compat subclasses (TCPServer -> BaseServer) their inherited methods
     * without per-row embedded_types arrays. */
    const char *alias_of;
} PyCompatType;

typedef struct {
    const char *qn;
    const char *short_name;
    const char *receiver; /* NULL for free functions */
    const char *ret;      /* return type text (parsed via py_parse_type_text) or NULL */
} PyCompatFunc;

static const PyCompatType kPyCompatTypes[] = {
    /* ── py2 builtins: aliases onto the py3 rows the table already has ── */
    {"builtins.unicode", "unicode", "builtins.str"},
    {"builtins.basestring", "basestring", "builtins.str"},
    {"builtins.long", "long", "builtins.int"},
    {"builtins.xrange", "xrange", "builtins.range"},
    {"builtins.buffer", "buffer", "builtins.memoryview"},
    {"builtins.file", "file", NULL},

    /* ── io.StringIO / io.BytesIO (absent from the generated io module) ── */
    {"io.StringIO", "StringIO", NULL},
    {"io.BytesIO", "BytesIO", NULL},

    /* ── configparser (3.x; also the ConfigParser py2 twin) ── */
    {"configparser.RawConfigParser", "RawConfigParser", NULL},
    {"configparser.ConfigParser", "ConfigParser", NULL},
    {"configparser.SectionProxy", "SectionProxy", NULL},
    {"configparser.Error", "Error", NULL},
    {"configparser.NoSectionError", "NoSectionError", "configparser.Error"},
    {"configparser.NoOptionError", "NoOptionError", "configparser.Error"},

    /* ── zoneinfo (3.9+) ── */
    {"zoneinfo.ZoneInfo", "ZoneInfo", NULL},

    /* ── asyncio.TaskGroup (3.11+) ── */
    {"asyncio.TaskGroup", "TaskGroup", NULL},
    {"asyncio.taskgroups.TaskGroup", "TaskGroup", "asyncio.TaskGroup"},

    /* ── socketserver (py3; also the SocketServer py2 twin) ── */
    {"socketserver.BaseServer", "BaseServer", NULL},
    {"socketserver.TCPServer", "TCPServer", "socketserver.BaseServer"},
    {"socketserver.UDPServer", "UDPServer", "socketserver.BaseServer"},
    {"socketserver.BaseRequestHandler", "BaseRequestHandler", NULL},
    {"socketserver.StreamRequestHandler", "StreamRequestHandler",
     "socketserver.BaseRequestHandler"},

    {NULL, NULL, NULL},
};

static const PyCompatFunc kPyCompatFuncs[] = {
    /* ── py2 builtin functions ── */
    {"builtins.raw_input", "raw_input", NULL, "str"},
    {"builtins.unichr", "unichr", NULL, "str"},
    {"builtins.cmp", "cmp", NULL, "int"},
    {"builtins.execfile", "execfile", NULL, NULL},
    {"builtins.reload", "reload", NULL, NULL},
    {"builtins.apply", "apply", NULL, NULL},
    {"builtins.intern", "intern", NULL, "str"},
    {"builtins.reduce", "reduce", NULL, NULL},
    {"builtins.coerce", "coerce", NULL, NULL},

    /* ── py2 dict iteration methods ── */
    {"builtins.dict.iteritems", "iteritems", "builtins.dict", NULL},
    {"builtins.dict.iterkeys", "iterkeys", "builtins.dict", NULL},
    {"builtins.dict.itervalues", "itervalues", "builtins.dict", NULL},
    {"builtins.dict.has_key", "has_key", "builtins.dict", "bool"},

    /* ── io.StringIO / io.BytesIO methods ── */
    {"io.StringIO.read", "read", "io.StringIO", "str"},
    {"io.StringIO.readline", "readline", "io.StringIO", "str"},
    {"io.StringIO.readlines", "readlines", "io.StringIO", "list[str]"},
    {"io.StringIO.write", "write", "io.StringIO", "int"},
    {"io.StringIO.getvalue", "getvalue", "io.StringIO", "str"},
    {"io.StringIO.seek", "seek", "io.StringIO", "int"},
    {"io.StringIO.close", "close", "io.StringIO", NULL},
    {"io.BytesIO.read", "read", "io.BytesIO", "bytes"},
    {"io.BytesIO.readline", "readline", "io.BytesIO", "bytes"},
    {"io.BytesIO.write", "write", "io.BytesIO", "int"},
    {"io.BytesIO.getvalue", "getvalue", "io.BytesIO", "bytes"},
    {"io.BytesIO.seek", "seek", "io.BytesIO", "int"},
    {"io.BytesIO.close", "close", "io.BytesIO", NULL},

    /* ── tomllib (3.11+) ── */
    {"tomllib.load", "load", NULL, "dict[str, object]"},
    {"tomllib.loads", "loads", NULL, "dict[str, object]"},

    /* ── configparser methods ── */
    {"configparser.ConfigParser.read", "read", "configparser.ConfigParser", "list[str]"},
    {"configparser.ConfigParser.read_string", "read_string", "configparser.ConfigParser", NULL},
    {"configparser.ConfigParser.read_file", "read_file", "configparser.ConfigParser", NULL},
    {"configparser.ConfigParser.readfp", "readfp", "configparser.ConfigParser", NULL},
    {"configparser.ConfigParser.get", "get", "configparser.ConfigParser", "str"},
    {"configparser.ConfigParser.getint", "getint", "configparser.ConfigParser", "int"},
    {"configparser.ConfigParser.getfloat", "getfloat", "configparser.ConfigParser", "float"},
    {"configparser.ConfigParser.getboolean", "getboolean", "configparser.ConfigParser", "bool"},
    {"configparser.ConfigParser.sections", "sections", "configparser.ConfigParser", "list[str]"},
    {"configparser.ConfigParser.options", "options", "configparser.ConfigParser", "list[str]"},
    {"configparser.ConfigParser.items", "items", "configparser.ConfigParser", NULL},
    {"configparser.ConfigParser.set", "set", "configparser.ConfigParser", NULL},
    {"configparser.ConfigParser.add_section", "add_section", "configparser.ConfigParser", NULL},
    {"configparser.ConfigParser.has_section", "has_section", "configparser.ConfigParser", "bool"},
    {"configparser.ConfigParser.has_option", "has_option", "configparser.ConfigParser", "bool"},
    {"configparser.ConfigParser.write", "write", "configparser.ConfigParser", NULL},

    /* ── asyncio.TaskGroup methods (3.11+) ── */
    {"asyncio.TaskGroup.create_task", "create_task", "asyncio.TaskGroup", NULL},
    {"asyncio.TaskGroup.__aenter__", "__aenter__", "asyncio.TaskGroup", "asyncio.TaskGroup"},
    {"asyncio.TaskGroup.__aexit__", "__aexit__", "asyncio.TaskGroup", NULL},

    /* ── socketserver methods ── */
    {"socketserver.BaseServer.serve_forever", "serve_forever", "socketserver.BaseServer", NULL},
    {"socketserver.BaseServer.shutdown", "shutdown", "socketserver.BaseServer", NULL},
    {"socketserver.BaseServer.handle_request", "handle_request", "socketserver.BaseServer", NULL},
    {"socketserver.BaseServer.server_close", "server_close", "socketserver.BaseServer", NULL},
    {"socketserver.BaseRequestHandler.handle", "handle", "socketserver.BaseRequestHandler", NULL},
    {"socketserver.BaseRequestHandler.setup", "setup", "socketserver.BaseRequestHandler", NULL},
    {"socketserver.BaseRequestHandler.finish", "finish", "socketserver.BaseRequestHandler", NULL},

    {NULL, NULL, NULL, NULL},
};

/* Register the compat tables. Idempotent per registry (rows are added once
 * per registry construction, exactly like cbm_python_stdlib_register). */
static void py_compat_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    if (!reg || !arena)
        return;
    for (int i = 0; kPyCompatTypes[i].qn; i++) {
        const PyCompatType *t = &kPyCompatTypes[i];
        CBMRegisteredType rt;
        memset(&rt, 0, sizeof(rt));
        rt.qualified_name = t->qn;
        rt.short_name = t->short_name;
        rt.alias_of = t->alias_of;
        cbm_registry_add_type(reg, rt);
    }
    for (int i = 0; kPyCompatFuncs[i].qn; i++) {
        const PyCompatFunc *f = &kPyCompatFuncs[i];
        CBMRegisteredFunc rf;
        memset(&rf, 0, sizeof(rf));
        rf.qualified_name = f->qn;
        rf.short_name = f->short_name;
        rf.receiver_type = f->receiver;
        if (f->ret) {
            const CBMType **rets =
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
            if (rets) {
                rets[0] = py_parse_type_text(arena, f->ret);
                rets[1] = NULL;
                rf.signature = cbm_type_func(arena, NULL, NULL, rets);
            }
        }
        cbm_registry_add_func(reg, rf);
    }
}
