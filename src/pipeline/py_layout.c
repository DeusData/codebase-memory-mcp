#include "pipeline/py_layout.h"
#include <ctype.h>
#include <string.h>

enum { PY_LAYOUT_SECTION_MAX = 64 };

bool cbm_py_layout_is_manifest(const char *basename) {
    return basename && (!strcmp(basename, "pyproject.toml") || !strcmp(basename, "setup.cfg") ||
                        !strcmp(basename, "setup.py") || !strcmp(basename, "tox.ini") ||
                        !strcmp(basename, "pytest.ini"));
}

const char *cbm_py_layout_manifest_at(size_t index) {
    static const char *const names[] = {"pyproject.toml", "setup.cfg", "setup.py", "tox.ini",
                                        "pytest.ini"};
    return index < sizeof(names) / sizeof(names[0]) ? names[index] : NULL;
}

static const char *py_past_spaces(const char *p, const char *end) {
    while (p < end && isspace((unsigned char)*p))
        p++;
    return p;
}

static const char *py_line_end(const char *p, const char *end) {
    const char *newline = p < end ? memchr(p, '\n', (size_t)(end - p)) : NULL;
    return newline ? newline : end;
}

static const char *py_skip_blank(const char *p, const char *end) {
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r'))
        p++;
    return p;
}

/* ── pyproject.toml ──────────────────────────────────────────────────── */

typedef enum {
    PY_FORM_STRINGS,    /* every string in the value is a root */
    PY_FORM_PARENTS,    /* every string names a package: its directory is the root */
    PY_FORM_AFTER_FROM, /* poetry's { include = "x", from = "lib" } */
    PY_FORM_AFTER_EMPTY /* setuptools' { "" = "src" } */
} py_form_t;

typedef struct {
    const char *section, *key;
    py_form_t form;
} py_toml_key_t;

static const py_toml_key_t py_toml_keys[] = {
    {"tool.setuptools.packages.find", "where", PY_FORM_STRINGS},
    {"tool.setuptools", "package-dir", PY_FORM_AFTER_EMPTY},
    {"tool.setuptools.package-dir", "\"\"", PY_FORM_STRINGS},
    {"tool.poetry", "packages", PY_FORM_AFTER_FROM},
    {"tool.poetry.packages", "from", PY_FORM_STRINGS},
    {"tool.hatch.build.targets.wheel", "packages", PY_FORM_PARENTS},
    {"tool.hatch.build", "packages", PY_FORM_PARENTS},
    {"tool.hatch.build.targets.wheel", "sources", PY_FORM_STRINGS},
    {"tool.hatch.build", "sources", PY_FORM_STRINGS},
    {"tool.pdm.build", "package-dir", PY_FORM_STRINGS},
    {"tool.maturin", "python-source", PY_FORM_STRINGS},
    {"tool.uv.build-backend", "module-root", PY_FORM_STRINGS},
    {"tool.pytest.ini_options", "pythonpath", PY_FORM_STRINGS},
    {"tool.ruff", "src", PY_FORM_STRINGS},
    {"tool.pyright", "extraPaths", PY_FORM_STRINGS},
    {"tool.mypy", "mypy_path", PY_FORM_STRINGS},
};

/* The end of a value: its line, or past the bracket that closes it. Strings and
 * comments cannot close one. */
static const char *py_toml_value_end(const char *value, const char *end) {
    if (value >= end || (*value != '[' && *value != '{'))
        return py_line_end(value, end);
    int depth = 0;
    char quote = 0;
    for (const char *p = value; p < end; p++) {
        if (quote) {
            quote = *p == quote ? 0 : (*p == '\n' ? 0 : quote);
            continue;
        }
        if (*p == '"' || *p == '\'') {
            quote = *p;
        } else if (*p == '#') {
            p = py_line_end(p, end);
            if (p >= end)
                break;
        } else if (*p == '[' || *p == '{') {
            depth++;
        } else if ((*p == ']' || *p == '}') && --depth == 0) {
            return p + 1;
        }
    }
    return end;
}

/* The strings of a value, in order, comments skipped. */
static void py_toml_strings(const char *value, const char *end, py_form_t form,
                            cbm_py_layout_emit_fn emit, void *context) {
    bool after_key = false;
    for (const char *p = value; p < end; p++) {
        if (*p == '#') {
            p = py_line_end(p, end);
            if (p >= end)
                return;
            continue;
        }
        if (form == PY_FORM_AFTER_FROM && end - p >= 4 && !memcmp(p, "from", 4)) {
            after_key = true;
            p += 3;
            continue;
        }
        if (*p != '"' && *p != '\'')
            continue;
        const char *close = p + 1;
        while (close < end && *close != *p && *close != '\n')
            close++;
        if (close >= end || *close != *p)
            return;
        size_t length = (size_t)(close - p - 1);
        const char *text = p + 1;
        p = close;
        switch (form) {
        case PY_FORM_STRINGS:
            emit(context, text, length);
            break;
        case PY_FORM_PARENTS: {
            size_t cut = length;
            while (cut && text[cut - 1] != '/')
                cut--;
            if (cut > 1)
                emit(context, text, cut - 1);
            break;
        }
        case PY_FORM_AFTER_FROM:
            if (after_key)
                emit(context, text, length);
            after_key = false;
            break;
        case PY_FORM_AFTER_EMPTY:
            if (after_key)
                emit(context, text, length);
            after_key = !length;
            break;
        }
    }
}

static bool py_toml_section(const char *line, const char *line_end, char *section, size_t size) {
    if (line >= line_end || *line != '[')
        return false;
    const char *p = line;
    while (p < line_end && *p == '[')
        p++;
    const char *close = p;
    while (close < line_end && *close != ']')
        close++;
    size_t length = (size_t)(close - p);
    if (length >= size) {
        section[0] = '\0';
        return true;
    }
    memcpy(section, p, length);
    section[length] = '\0';
    return true;
}

static void py_parse_toml(const char *source, const char *end, cbm_py_layout_emit_fn emit,
                          void *context) {
    char section[PY_LAYOUT_SECTION_MAX] = "";
    for (const char *line = source; line < end;) {
        const char *start = py_skip_blank(line, end);
        const char *line_end = py_line_end(start, end);
        const char *next = line_end < end ? line_end + 1 : end;
        if (py_toml_section(start, line_end, section, sizeof(section))) {
            line = next;
            continue;
        }
        for (size_t i = 0; i < sizeof(py_toml_keys) / sizeof(py_toml_keys[0]); i++) {
            const py_toml_key_t *key = &py_toml_keys[i];
            size_t length = strlen(key->key);
            if (strcmp(section, key->section) || (size_t)(line_end - start) < length ||
                memcmp(start, key->key, length))
                continue;
            const char *value = py_skip_blank(start + length, line_end);
            if (value >= line_end || *value != '=')
                continue;
            value = py_skip_blank(value + 1, line_end);
            const char *value_end = py_toml_value_end(value, end);
            if (value < line_end && (*value != '{' || key->form == PY_FORM_AFTER_EMPTY))
                py_toml_strings(value, value_end, key->form, emit, context);
            if (value_end > next)
                next = py_line_end(value_end, end) < end ? py_line_end(value_end, end) + 1 : end;
            break;
        }
        line = next;
    }
}

/* ── setup.cfg ───────────────────────────────────────────────────────── */

static void py_cfg_tokens(const char *p, const char *end, cbm_py_layout_emit_fn emit,
                          void *context) {
    while (p < end) {
        while (p < end && isspace((unsigned char)*p))
            p++;
        const char *token = p;
        while (p < end && !isspace((unsigned char)*p))
            p++;
        if (p > token)
            emit(context, token, (size_t)(p - token));
    }
}

/* One line of a value: on the key's line, or a continuation below it. */
static void py_cfg_value(const char *section, const char *key, size_t key_length, const char *text,
                         const char *end, cbm_py_layout_emit_fn emit, void *context) {
    text = py_skip_blank(text, end);
    if (!strcmp(section, "options") && key_length == 11 && !memcmp(key, "package_dir", 11)) {
        /* `= src` maps the root package; `name = dir` maps one package only. */
        if (text < end && *text == '=')
            py_cfg_tokens(text + 1, end, emit, context);
        return;
    }
    if ((!strcmp(section, "options.packages.find") && key_length == 5 &&
         !memcmp(key, "where", 5)) ||
        ((!strcmp(section, "tool:pytest") || !strcmp(section, "pytest")) && key_length == 10 &&
         !memcmp(key, "pythonpath", 10)))
        py_cfg_tokens(text, end, emit, context);
}

static void py_parse_cfg(const char *source, const char *end, cbm_py_layout_emit_fn emit,
                         void *context) {
    char section[PY_LAYOUT_SECTION_MAX] = "";
    const char *key = NULL;
    size_t key_length = 0;
    for (const char *line = source; line < end;) {
        const char *line_end = py_line_end(line, end);
        const char *next = line_end < end ? line_end + 1 : end;
        const char *start = py_skip_blank(line, line_end);
        if (start >= line_end || *start == '#' || *start == ';') {
            line = next;
            continue;
        }
        if (start == line && py_toml_section(start, line_end, section, sizeof(section))) {
            key = NULL;
        } else if (start != line) {
            if (key)
                py_cfg_value(section, key, key_length, start, line_end, emit, context);
        } else {
            const char *equals = memchr(start, '=', (size_t)(line_end - start));
            key = start;
            const char *stop = equals ? equals : line_end;
            while (stop > key && (stop[-1] == ' ' || stop[-1] == '\t'))
                stop--;
            key_length = (size_t)(stop - key);
            if (equals)
                py_cfg_value(section, key, key_length, equals + 1, line_end, emit, context);
        }
        line = next;
    }
}

/* ── setup.py ────────────────────────────────────────────────────────── */

static const char *py_find(const char *p, const char *end, const char *text) {
    size_t length = strlen(text);
    for (; p && (size_t)(end - p) >= length; p++) {
        p = memchr(p, text[0], (size_t)(end - p));
        if (!p || (size_t)(end - p) < length)
            return NULL;
        if (!memcmp(p, text, length))
            return p;
    }
    return NULL;
}

static const char *py_literal(const char *p, const char *end, const char **text, size_t *length) {
    p = p < end ? p : end;
    while (p < end && isspace((unsigned char)*p))
        p++;
    if (p >= end || (*p != '"' && *p != '\''))
        return NULL;
    const char *close = p + 1;
    while (close < end && *close != *p && *close != '\n')
        close++;
    if (close >= end || *close != *p)
        return NULL;
    *text = p + 1;
    *length = (size_t)(close - p - 1);
    return close + 1;
}

/* `package_dir={"": "src"}` and `find_packages("src")`, as literals. */
static void py_parse_setup_py(const char *source, const char *end, cbm_py_layout_emit_fn emit,
                              void *context) {
    const char *text = NULL;
    size_t length = 0;
    for (const char *p = py_find(source, end, "package_dir"); p;
         p = py_find(p + 1, end, "package_dir")) {
        const char *q = p + strlen("package_dir");
        while (q < end && isspace((unsigned char)*q))
            q++;
        if (q >= end || *q != '=')
            continue;
        q++;
        while (q < end && isspace((unsigned char)*q))
            q++;
        if (q >= end || *q != '{')
            continue;
        q = py_literal(q + 1, end, &text, &length);
        if (!q || length)
            continue;
        while (q < end && isspace((unsigned char)*q))
            q++;
        if (q >= end || *q != ':')
            continue;
        if (py_literal(q + 1, end, &text, &length))
            emit(context, text, length);
    }
    static const char *const finders[] = {"find_packages(", "find_namespace_packages("};
    for (size_t f = 0; f < sizeof(finders) / sizeof(finders[0]); f++) {
        for (const char *p = py_find(source, end, finders[f]); p;
             p = py_find(p + 1, end, finders[f])) {
            const char *q = py_past_spaces(p + strlen(finders[f]), end);
            if (end - q > 5 && !memcmp(q, "where", 5)) {
                q = py_past_spaces(q + 5, end);
                if (q >= end || *q != '=')
                    continue;
                q++;
            }
            if (py_literal(q, end, &text, &length))
                emit(context, text, length);
        }
    }
}

void cbm_py_layout_parse(const char *basename, const char *source, size_t source_len,
                         cbm_py_layout_emit_fn emit, void *context) {
    const char *end = source + source_len;
    if (!strcmp(basename, "pyproject.toml"))
        py_parse_toml(source, end, emit, context);
    else if (!strcmp(basename, "setup.cfg") || !strcmp(basename, "tox.ini") ||
             !strcmp(basename, "pytest.ini"))
        py_parse_cfg(source, end, emit, context);
    else if (!strcmp(basename, "setup.py"))
        py_parse_setup_py(source, end, emit, context);
}

void cbm_py_layout_workspace_members(const char *source, size_t source_len,
                                     cbm_py_layout_emit_fn emit, void *context) {
    const char *end = source + source_len;
    char section[PY_LAYOUT_SECTION_MAX] = "";
    for (const char *line = source; line < end;) {
        const char *start = py_skip_blank(line, end);
        const char *line_end = py_line_end(start, end);
        const char *next = line_end < end ? line_end + 1 : end;
        if (py_toml_section(start, line_end, section, sizeof(section))) {
            line = next;
            continue;
        }
        if (!strcmp(section, "tool.uv.workspace") && line_end - start > 7 &&
            !memcmp(start, "members", 7)) {
            const char *value = py_skip_blank(start + 7, line_end);
            if (value < line_end && *value == '=') {
                value = py_skip_blank(value + 1, line_end);
                const char *value_end = py_toml_value_end(value, end);
                py_toml_strings(value, value_end, PY_FORM_STRINGS, emit, context);
                return;
            }
        }
        line = next;
    }
}

bool cbm_py_layout_member_matches(const char *pattern, const char *directory) {
    size_t length = strlen(pattern);
    if (length < 2 || strcmp(pattern + length - 2, "/*"))
        return !strcmp(pattern, directory);
    return !strncmp(pattern, directory, length - 1) && directory[length - 1] &&
           !strchr(directory + length - 1, '/');
}

/* ── Standard library ────────────────────────────────────────────────── */

/* Sorted. CPython's sys.stdlib_module_names, top-level public modules. */
static const char *const py_stdlib_modules[] = {
    "abc",
    "aifc",
    "antigravity",
    "argparse",
    "array",
    "ast",
    "asynchat",
    "asyncio",
    "asyncore",
    "atexit",
    "audioop",
    "base64",
    "bdb",
    "binascii",
    "bisect",
    "builtins",
    "bz2",
    "cProfile",
    "calendar",
    "cgi",
    "cgitb",
    "chunk",
    "cmath",
    "cmd",
    "code",
    "codecs",
    "codeop",
    "collections",
    "colorsys",
    "compileall",
    "concurrent",
    "configparser",
    "contextlib",
    "contextvars",
    "copy",
    "copyreg",
    "crypt",
    "csv",
    "ctypes",
    "curses",
    "dataclasses",
    "datetime",
    "dbm",
    "decimal",
    "difflib",
    "dis",
    "distutils",
    "doctest",
    "email",
    "encodings",
    "ensurepip",
    "enum",
    "errno",
    "faulthandler",
    "fcntl",
    "filecmp",
    "fileinput",
    "fnmatch",
    "fractions",
    "ftplib",
    "functools",
    "gc",
    "genericpath",
    "getopt",
    "getpass",
    "gettext",
    "glob",
    "graphlib",
    "grp",
    "gzip",
    "hashlib",
    "heapq",
    "hmac",
    "html",
    "http",
    "idlelib",
    "imaplib",
    "imghdr",
    "imp",
    "importlib",
    "inspect",
    "io",
    "ipaddress",
    "itertools",
    "json",
    "keyword",
    "lib2to3",
    "linecache",
    "locale",
    "logging",
    "lzma",
    "mailbox",
    "mailcap",
    "marshal",
    "math",
    "mimetypes",
    "mmap",
    "modulefinder",
    "msilib",
    "msvcrt",
    "multiprocessing",
    "netrc",
    "nis",
    "nntplib",
    "nt",
    "ntpath",
    "nturl2path",
    "numbers",
    "opcode",
    "operator",
    "optparse",
    "os",
    "ossaudiodev",
    "pathlib",
    "pdb",
    "pickle",
    "pickletools",
    "pipes",
    "pkgutil",
    "platform",
    "plistlib",
    "poplib",
    "posix",
    "posixpath",
    "pprint",
    "profile",
    "pstats",
    "pty",
    "pwd",
    "py_compile",
    "pyclbr",
    "pydoc",
    "pydoc_data",
    "pyexpat",
    "queue",
    "quopri",
    "random",
    "re",
    "readline",
    "reprlib",
    "resource",
    "rlcompleter",
    "runpy",
    "sched",
    "secrets",
    "select",
    "selectors",
    "shelve",
    "shlex",
    "shutil",
    "signal",
    "site",
    "smtpd",
    "smtplib",
    "sndhdr",
    "socket",
    "socketserver",
    "spwd",
    "sqlite3",
    "sre_compile",
    "sre_constants",
    "sre_parse",
    "ssl",
    "stat",
    "statistics",
    "string",
    "stringprep",
    "struct",
    "subprocess",
    "sunau",
    "symtable",
    "sys",
    "sysconfig",
    "syslog",
    "tabnanny",
    "tarfile",
    "telnetlib",
    "tempfile",
    "termios",
    "textwrap",
    "this",
    "threading",
    "time",
    "timeit",
    "tkinter",
    "token",
    "tokenize",
    "tomllib",
    "trace",
    "traceback",
    "tracemalloc",
    "tty",
    "turtle",
    "turtledemo",
    "types",
    "typing",
    "unicodedata",
    "unittest",
    "urllib",
    "uu",
    "uuid",
    "venv",
    "warnings",
    "wave",
    "weakref",
    "webbrowser",
    "winreg",
    "winsound",
    "wsgiref",
    "xdrlib",
    "xml",
    "xmlrpc",
    "zipapp",
    "zipfile",
    "zipimport",
    "zlib",
    "zoneinfo",
};

bool cbm_py_layout_is_stdlib(const char *name, size_t length) {
    size_t low = 0, high = sizeof(py_stdlib_modules) / sizeof(py_stdlib_modules[0]);
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        int order = strncmp(py_stdlib_modules[middle], name, length);
        if (!order && py_stdlib_modules[middle][length])
            order = 1;
        if (!order)
            return true;
        if (order < 0)
            low = middle + 1;
        else
            high = middle;
    }
    return false;
}
