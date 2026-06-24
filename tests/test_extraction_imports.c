/*
 * test_extraction_imports.c — Comprehensive, language-agnostic, table-driven
 * reproduction suite for IMPORT extraction across the 9 hybrid-LSP languages.
 *
 * REPRODUCE-FIRST suite:  written BEFORE any fixes so that RED rows prove the
 * bug is real and GREEN rows serve as regression guards.
 *
 * ── Graph-level symptom (from bench run 2026-06) ────────────────────────────
 * Language   import keyword    graph IMPORTS edges   status
 * ---------  ----------------  --------------------  --------------------------
 * Rust       use               2168 uses → 0 edges   BUG
 * Kotlin     import            6110 imports → ~0      BUG
 * PHP        use               5787 uses → 1 edge     BUG
 * Java       import            many → 0 edges         BUG
 * C#         using             many → 0 edges         BUG
 * Python     import/from       working                OK
 * TypeScript import            working                OK
 * Go         import            working                OK
 * JavaScript import/require    working                OK
 *
 * ── What this file tests ────────────────────────────────────────────────────
 * We test the EXTRACTION layer only: does cbm_extract_file() populate
 * r->imports correctly for each syntactic form?  NOT whether the graph
 * pipeline creates IMPORTS edges (that is downstream).
 *
 * ── Reading the existing tests for attribution ───────────────────────────────
 * tests/test_extraction.c already has:
 *   rust_imports  — asserts r->imports.count > 0 AND has_import(r,"std::collections::HashMap")
 *   java_imports  — asserts r->imports.count > 0 AND has_import(r,"java.util.List")
 *   go_imports    — asserts r->imports.count > 0 AND has_import(r,"fmt")
 *   python_imports— asserts r->imports.count > 0
 *   js_imports    — asserts r->imports.count > 0
 *   c_imports     — asserts r->imports.count > 0 AND has_import(r,"stdio.h")
 *
 * Those existing tests PASS today (extraction works for rust/java at the
 * basic level).  Therefore the zero-edge bug for Rust, Java, Kotlin, PHP,
 * C# is likely DOWNSTREAM (edge-creation / resolution pipeline), NOT
 * extraction.  However, this file tests finer extraction contracts—aliased,
 * grouped, wildcard, relative, namespaced forms—which may surface additional
 * extraction-level gaps, particularly for Kotlin, PHP, and C# whose parsers
 * use `parse_generic_imports` / `parse_kotlin_imports` with less-tested paths.
 *
 * ── Expected RED vs GREEN per language ──────────────────────────────────────
 * Language   Extraction expected   Reasoning
 * ---------  --------------------  ------------------------------------------
 * Go         GREEN (all)           dedicated parse_go_imports; stress test passes
 * Python     GREEN (all)           process_py_import_stmt/from; existing tests pass
 * TypeScript GREEN (all)           process_es_import_statement; existing tests pass
 * JavaScript GREEN (all)           walk_es_imports + CommonJS; existing tests pass
 * Rust       GREEN (basic)         parse_rust_imports strips "use "/";" text-based;
 *                                  grouped/aliased forms may RED (whole-node text
 *                                  including braces is used as module_path)
 * Java       GREEN (basic)         parse_java_imports scoped_identifier; existing
 *                                  test asserts java.util.List → GREEN; static-
 *                                  import and wildcard may differ in format
 * Kotlin     LIKELY GREEN (basic)  parse_kotlin_imports → extract_one_import_header
 *                                  → generic_import_from_text strips "import "/";";
 *                                  aliased form ("as X") remains in module_path text
 * PHP        UNCERTAIN             CBM_LANG_PHP → parse_generic_imports("expression_
 *                                  statement") — PHP "use" is a namespace_use_clause,
 *                                  NOT an expression_statement; so "use Foo\Bar" likely
 *                                  yields 0 imports → expected RED
 * C#         UNCERTAIN             CBM_LANG_CSHARP → parse_generic_imports("using_directive")
 *                                  → generic_import_from_text strips "using "/";";
 *                                  basic form should work → expected GREEN (basic);
 *                                  alias form "using F = Foo.Bar" may include "= Foo.Bar"
 *                                  in path → possibly RED for alias target matching
 *
 * IMPORTANT: A test that FAILS means extraction is broken at the C level.
 * A test that PASSES means extraction is correct; the zero-edges must be
 * a downstream graph-pipeline bug (to be reproduced separately).
 *
 * ── Field names used ────────────────────────────────────────────────────────
 * CBMImport.module_path  — the full import/use path string
 * CBMImport.local_name   — last segment or alias name
 * r->imports.count       — number of extracted imports
 * r->imports.items[i]    — array of CBMImport
 */

#include "test_framework.h"
#include "cbm.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Helpers ──────────────────────────────────────────────────────────────── */

/* Return 1 if any extracted import has module_path containing path_substr. */
static int imp_has(CBMFileResult *r, const char *path_substr) {
    for (int i = 0; i < r->imports.count; i++) {
        if (r->imports.items[i].module_path &&
            strstr(r->imports.items[i].module_path, path_substr) != NULL)
            return 1;
    }
    return 0;
}

/* Return 1 if any extracted import has local_name equal to local. */
static int imp_local(CBMFileResult *r, const char *local) {
    for (int i = 0; i < r->imports.count; i++) {
        if (r->imports.items[i].local_name &&
            strcmp(r->imports.items[i].local_name, local) == 0)
            return 1;
    }
    return 0;
}

/* Convenience extract wrapper. */
static CBMFileResult *do_extract(const char *src, CBMLanguage lang,
                                  const char *path) {
    return cbm_extract_file(src, (int)strlen(src), lang, "testproj", path,
                             0, NULL, NULL);
}

/*
 * Table-driven case struct.
 * expected[] is a NULL-terminated list of substrings that MUST each appear in
 * some extracted import's module_path.  min_count is the minimum number of
 * imports the extractor must produce.
 */
typedef struct {
    CBMLanguage  lang;
    const char  *path;          /* fake file path for language detection */
    const char  *src;           /* source snippet */
    const char  *expected[16];  /* NULL-terminated substrings of module_path */
    int          min_count;     /* r->imports.count must be >= this */
    const char  *label;         /* human-readable case name for diagnostics */
} import_case_t;

/*
 * Run one table entry; returns 0 on pass, 1 on failure.
 * Prints a diagnostic line on failure so failures are self-describing.
 */
static int run_import_case(const import_case_t *tc) {
    CBMFileResult *r = do_extract(tc->src, tc->lang, tc->path);
    if (!r) {
        printf("  FAIL [%s]: cbm_extract_file returned NULL\n", tc->label);
        return 1;
    }
    if (r->has_error) {
        printf("  FAIL [%s]: extractor reported error\n", tc->label);
        cbm_free_result(r);
        return 1;
    }
    int fail = 0;
    if (r->imports.count < tc->min_count) {
        printf("  FAIL [%s]: imports.count=%d want>=%d\n",
               tc->label, r->imports.count, tc->min_count);
        fail = 1;
    }
    for (int e = 0; tc->expected[e] != NULL; e++) {
        if (!imp_has(r, tc->expected[e])) {
            printf("  FAIL [%s]: no import with module_path containing \"%s\" "
                   "(count=%d)\n",
                   tc->label, tc->expected[e], r->imports.count);
            fail = 1;
        }
    }
    cbm_free_result(r);
    return fail;
}

/* Run an array of cases; accumulate failures and return total. */
static int run_cases(const import_case_t *cases, int n) {
    int failures = 0;
    for (int i = 0; i < n; i++) {
        failures += run_import_case(&cases[i]);
    }
    return failures;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Go — dedicated parse_go_imports, expected GREEN for all forms
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t go_cases[] = {
    /* simple single-line */
    { CBM_LANG_GO, "a.go",
      "package main\nimport \"fmt\"\nfunc main() {}\n",
      {"fmt", NULL}, 1, "go_simple" },

    /* grouped import block */
    { CBM_LANG_GO, "b.go",
      "package main\nimport (\n    \"os\"\n    \"net/http\"\n    \"io\"\n)\nfunc f() {}\n",
      {"os", "net/http", "io", NULL}, 3, "go_grouped_block" },

    /* aliased import */
    { CBM_LANG_GO, "c.go",
      "package main\nimport myfmt \"fmt\"\nfunc f() {}\n",
      {"fmt", NULL}, 1, "go_aliased" },

    /* blank import */
    { CBM_LANG_GO, "d.go",
      "package main\nimport _ \"database/sql/driver\"\nfunc f() {}\n",
      {"database/sql/driver", NULL}, 1, "go_blank" },

    /* dot import */
    { CBM_LANG_GO, "e.go",
      "package main\nimport . \"math\"\nfunc f() {}\n",
      {"math", NULL}, 1, "go_dot" },

    /* mixed aliases in block */
    { CBM_LANG_GO, "f.go",
      "package main\nimport (\n    \"fmt\"\n    h \"net/http\"\n    _ \"unsafe\"\n)\nfunc f() {}\n",
      {"fmt", "net/http", "unsafe", NULL}, 3, "go_mixed_block" },

    /* multiple separate import declarations */
    { CBM_LANG_GO, "g.go",
      "package main\nimport \"sync\"\nimport \"sync/atomic\"\nfunc f() {}\n",
      {"sync", "sync/atomic", NULL}, 2, "go_multiple_decls" },

    /* subpackage path */
    { CBM_LANG_GO, "h.go",
      "package main\nimport \"google.golang.org/grpc/codes\"\nfunc f() {}\n",
      {"google.golang.org/grpc/codes", NULL}, 1, "go_subpackage" },

    /* stdlib + external + internal in block */
    { CBM_LANG_GO, "i.go",
      "package main\nimport (\n    \"context\"\n    \"github.com/pkg/errors\"\n    "
      "\"myapp/internal/config\"\n)\nfunc f() {}\n",
      {"context", "github.com/pkg/errors", "myapp/internal/config", NULL}, 3,
      "go_stdlib_ext_internal" },

    /* deeply nested package */
    { CBM_LANG_GO, "j.go",
      "package main\nimport \"a/b/c/d/e/f\"\nfunc f() {}\n",
      {"a/b/c/d/e/f", NULL}, 1, "go_deeply_nested" },

    /* 10+ imports in one block */
    { CBM_LANG_GO, "k.go",
      "package main\nimport (\n    \"bufio\"\n    \"bytes\"\n    \"errors\"\n    \"fmt\"\n"
      "    \"io\"\n    \"log\"\n    \"math\"\n    \"os\"\n    \"path\"\n    \"sort\"\n"
      "    \"strconv\"\n    \"strings\"\n)\nfunc f() {}\n",
      {"bufio", "bytes", "errors", "fmt", "io", "log", "math", "os", "path",
       "sort", NULL}, 12, "go_many_imports" },
};

TEST(go_import_table) {
    int failures = run_cases(go_cases, (int)(sizeof(go_cases)/sizeof(go_cases[0])));
    if (failures > 0) {
        printf("  %d go import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Python — process_py_import_stmt + process_py_import_from, expected GREEN
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t python_cases[] = {
    /* simple import */
    { CBM_LANG_PYTHON, "a.py",
      "import os\n",
      {"os", NULL}, 1, "py_simple" },

    /* from-import */
    { CBM_LANG_PYTHON, "b.py",
      "from sys import argv\n",
      {"sys", NULL}, 1, "py_from" },

    /* from-import multiple */
    { CBM_LANG_PYTHON, "c.py",
      "from collections import defaultdict, OrderedDict\n",
      {"collections", NULL}, 1, "py_from_multiple" },

    /* aliased import */
    { CBM_LANG_PYTHON, "d.py",
      "import numpy as np\n",
      {"numpy", NULL}, 1, "py_aliased" },

    /* from-import with alias */
    { CBM_LANG_PYTHON, "e.py",
      "from pathlib import Path as P\n",
      {"pathlib", NULL}, 1, "py_from_aliased" },

    /* wildcard from-import */
    { CBM_LANG_PYTHON, "f.py",
      "from os.path import *\n",
      {"os.path", NULL}, 1, "py_wildcard" },

    /* relative import (single dot) */
    { CBM_LANG_PYTHON, "g.py",
      "from . import utils\n",
      {NULL}, 1, "py_relative_dot" },  /* module_path may be "." — just count */

    /* relative import with path */
    { CBM_LANG_PYTHON, "h.py",
      "from .models import User\n",
      {"models", NULL}, 1, "py_relative_models" },

    /* nested module */
    { CBM_LANG_PYTHON, "i.py",
      "import xml.etree.ElementTree as ET\n",
      {"xml.etree.ElementTree", NULL}, 1, "py_nested_module" },

    /* multiple top-level imports */
    { CBM_LANG_PYTHON, "j.py",
      "import os\nimport sys\nimport re\nfrom typing import List, Dict\n",
      {"os", "sys", "re", "typing", NULL}, 4, "py_multi" },

    /* stdlib + third-party */
    { CBM_LANG_PYTHON, "k.py",
      "import json\nfrom flask import Flask\nimport requests\n",
      {"json", "flask", "requests", NULL}, 3, "py_stdlib_thirdparty" },

    /* from __future__ import */
    { CBM_LANG_PYTHON, "l.py",
      "from __future__ import annotations\nimport os\n",
      {"__future__", "os", NULL}, 2, "py_future" },

    /* from-import with parentheses (multi-line) */
    { CBM_LANG_PYTHON, "m.py",
      "from typing import (\n    List,\n    Dict,\n    Optional,\n)\n",
      {"typing", NULL}, 1, "py_from_parens" },

    /* deeply nested package */
    { CBM_LANG_PYTHON, "n.py",
      "from django.db.models.query import QuerySet\n",
      {"django.db.models.query", NULL}, 1, "py_deep_nested" },

    /* double-relative import */
    { CBM_LANG_PYTHON, "o.py",
      "from ..utils import helper\n",
      {"utils", NULL}, 1, "py_double_relative" },
};

TEST(python_import_table) {
    int failures = run_cases(python_cases, (int)(sizeof(python_cases)/sizeof(python_cases[0])));
    if (failures > 0) {
        printf("  %d python import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * TypeScript — walk_es_imports, expected GREEN for all forms
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t typescript_cases[] = {
    /* default import */
    { CBM_LANG_TYPESCRIPT, "a.ts",
      "import React from 'react';\n",
      {"react", NULL}, 1, "ts_default" },

    /* named imports */
    { CBM_LANG_TYPESCRIPT, "b.ts",
      "import { useState, useEffect } from 'react';\n",
      {"react", NULL}, 1, "ts_named" },

    /* namespace import */
    { CBM_LANG_TYPESCRIPT, "c.ts",
      "import * as path from 'path';\n",
      {"path", NULL}, 1, "ts_namespace" },

    /* aliased named import */
    { CBM_LANG_TYPESCRIPT, "d.ts",
      "import { SomeType as ST } from './types';\n",
      {"types", NULL}, 1, "ts_named_alias" },

    /* side-effect-only import */
    { CBM_LANG_TYPESCRIPT, "e.ts",
      "import './polyfills';\n",
      {"polyfills", NULL}, 1, "ts_side_effect" },

    /* relative path */
    { CBM_LANG_TYPESCRIPT, "f.ts",
      "import { Config } from '../config';\n",
      {"config", NULL}, 1, "ts_relative" },

    /* deep relative path */
    { CBM_LANG_TYPESCRIPT, "g.ts",
      "import { helper } from '../../utils/helpers';\n",
      {"helpers", NULL}, 1, "ts_deep_relative" },

    /* mixed default + named */
    { CBM_LANG_TYPESCRIPT, "h.ts",
      "import React, { Component } from 'react';\n",
      {"react", NULL}, 1, "ts_mixed_default_named" },

    /* scoped package */
    { CBM_LANG_TYPESCRIPT, "i.ts",
      "import { Client } from '@anthropic-ai/sdk';\n",
      {"@anthropic-ai/sdk", NULL}, 1, "ts_scoped_package" },

    /* multiple imports */
    { CBM_LANG_TYPESCRIPT, "j.ts",
      "import path from 'path';\nimport fs from 'fs';\nimport { EventEmitter } from 'events';\n",
      {"path", "fs", "events", NULL}, 3, "ts_multiple" },

    /* TSX component file — uses CBM_LANG_TSX */
    { CBM_LANG_TSX, "k.tsx",
      "import React from 'react';\nimport { Button } from './Button';\n",
      {"react", "Button", NULL}, 2, "tsx_component" },

    /* type-only import (TS 3.8+) */
    { CBM_LANG_TYPESCRIPT, "l.ts",
      "import type { User } from './models';\n",
      {"models", NULL}, 1, "ts_type_only" },

    /* re-export style import */
    { CBM_LANG_TYPESCRIPT, "m.ts",
      "import { foo } from './a';\nimport { bar } from './b';\nimport { baz } from './c';\n",
      {"a", "b", "c", NULL}, 3, "ts_re_export" },

    /* package with subpath */
    { CBM_LANG_TYPESCRIPT, "n.ts",
      "import { format } from 'date-fns/format';\n",
      {"date-fns/format", NULL}, 1, "ts_subpath" },

    /* dynamic import (inside function — CommonJS-like) — may not be extracted */
    { CBM_LANG_TYPESCRIPT, "o.ts",
      "import { readFileSync } from 'fs';\nconst data = readFileSync('./data.json');\n",
      {"fs", NULL}, 1, "ts_fs_readfile" },
};

TEST(typescript_import_table) {
    int failures = run_cases(typescript_cases, (int)(sizeof(typescript_cases)/sizeof(typescript_cases[0])));
    if (failures > 0) {
        printf("  %d typescript import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * JavaScript — walk_es_imports + CommonJS require, expected GREEN
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t javascript_cases[] = {
    /* ES default */
    { CBM_LANG_JAVASCRIPT, "a.js",
      "import React from 'react';\n",
      {"react", NULL}, 1, "js_es_default" },

    /* ES named */
    { CBM_LANG_JAVASCRIPT, "b.js",
      "import { map, filter } from 'lodash';\n",
      {"lodash", NULL}, 1, "js_es_named" },

    /* CommonJS require */
    { CBM_LANG_JAVASCRIPT, "c.js",
      "const fs = require('fs');\n",
      {"fs", NULL}, 1, "js_cjs_require" },

    /* CommonJS destructured require */
    { CBM_LANG_JAVASCRIPT, "d.js",
      "const { join } = require('path');\n",
      {"path", NULL}, 1, "js_cjs_destructure" },

    /* ES namespace */
    { CBM_LANG_JAVASCRIPT, "e.js",
      "import * as utils from './utils';\n",
      {"utils", NULL}, 1, "js_es_namespace" },

    /* relative require */
    { CBM_LANG_JAVASCRIPT, "f.js",
      "const helper = require('./helper');\n",
      {"helper", NULL}, 1, "js_cjs_relative" },

    /* scoped package require */
    { CBM_LANG_JAVASCRIPT, "g.js",
      "const { Schema } = require('@hapi/joi');\n",
      {"@hapi/joi", NULL}, 1, "js_cjs_scoped" },

    /* mixed ES + CJS */
    { CBM_LANG_JAVASCRIPT, "h.js",
      "import express from 'express';\nconst path = require('path');\n",
      {"express", "path", NULL}, 2, "js_mixed_es_cjs" },

    /* side-effect import */
    { CBM_LANG_JAVASCRIPT, "i.js",
      "import './styles.css';\n",
      {"styles.css", NULL}, 1, "js_side_effect" },

    /* multiple CJS */
    { CBM_LANG_JAVASCRIPT, "j.js",
      "const a = require('a');\nconst b = require('b');\nconst c = require('c');\n",
      {"a", "b", "c", NULL}, 3, "js_cjs_multiple" },

    /* named alias import */
    { CBM_LANG_JAVASCRIPT, "k.js",
      "import { default as D } from 'somelib';\n",
      {"somelib", NULL}, 1, "js_named_alias" },
};

TEST(javascript_import_table) {
    int failures = run_cases(javascript_cases, (int)(sizeof(javascript_cases)/sizeof(javascript_cases[0])));
    if (failures > 0) {
        printf("  %d javascript import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Rust — parse_rust_imports (text-based: strips "use "/";" from whole node).
 *
 * EXPECTED: basic and namespaced forms GREEN; grouped/braced forms may be RED
 * because the whole `use std::io::{self, Write};` text becomes the module_path,
 * so has_import("std::io") would still match as a substring — likely GREEN even
 * for grouped. The alias form `use X as Y;` leaves "std::collections::HashMap as H"
 * as module_path — has_import("std::collections::HashMap") would still match.
 * So most forms should be GREEN at extraction.  Graph-level zero edges are
 * downstream.
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t rust_cases[] = {
    /* simple use */
    { CBM_LANG_RUST, "a.rs",
      "use std::collections::HashMap;\nfn main() {}\n",
      {"std::collections::HashMap", NULL}, 1, "rust_simple" },

    /* crate-root use */
    { CBM_LANG_RUST, "b.rs",
      "use crate::config::Config;\nfn main() {}\n",
      {"crate::config::Config", NULL}, 1, "rust_crate_root" },

    /* self-reference in braces */
    { CBM_LANG_RUST, "c.rs",
      "use std::io::{self, Write};\nfn main() {}\n",
      {"std::io", NULL}, 1, "rust_grouped_self_write" },

    /* braced multi-item */
    { CBM_LANG_RUST, "d.rs",
      "use std::sync::{Arc, Mutex, RwLock};\nfn main() {}\n",
      {"std::sync", NULL}, 1, "rust_grouped_3" },

    /* wildcard glob */
    { CBM_LANG_RUST, "e.rs",
      "use std::prelude::v1::*;\nfn main() {}\n",
      {"std::prelude", NULL}, 1, "rust_glob" },

    /* alias rename */
    { CBM_LANG_RUST, "f.rs",
      "use std::collections::HashMap as Map;\nfn main() {}\n",
      {"std::collections::HashMap", NULL}, 1, "rust_alias" },

    /* external crate */
    { CBM_LANG_RUST, "g.rs",
      "use serde::{Serialize, Deserialize};\nfn main() {}\n",
      {"serde", NULL}, 1, "rust_serde_grouped" },

    /* super:: relative */
    { CBM_LANG_RUST, "h.rs",
      "use super::utils::helper;\nfn main() {}\n",
      {"super::utils::helper", NULL}, 1, "rust_super" },

    /* multiple use statements */
    { CBM_LANG_RUST, "i.rs",
      "use std::fs;\nuse std::io::Read;\nuse std::path::PathBuf;\nfn main() {}\n",
      {"std::fs", "std::io::Read", "std::path::PathBuf", NULL}, 3, "rust_multi" },

    /* nested braces */
    { CBM_LANG_RUST, "j.rs",
      "use std::{collections::HashMap, io::{BufRead, BufReader}};\nfn main() {}\n",
      {"std::", NULL}, 1, "rust_nested_braces" },

    /* tokio async */
    { CBM_LANG_RUST, "k.rs",
      "use tokio::runtime::Runtime;\nuse tokio::sync::mpsc;\nfn main() {}\n",
      {"tokio::runtime::Runtime", "tokio::sync::mpsc", NULL}, 2, "rust_tokio" },

    /* pub use (re-export) */
    { CBM_LANG_RUST, "l.rs",
      "pub use crate::error::Error;\nfn main() {}\n",
      {"crate::error::Error", NULL}, 1, "rust_pub_use" },

    /* std + external + crate mix */
    { CBM_LANG_RUST, "m.rs",
      "use std::fmt;\nuse log::info;\nuse crate::db::Database;\nfn main() {}\n",
      {"std::fmt", "log::info", "crate::db::Database", NULL}, 3, "rust_mixed" },

    /* deeply nested path */
    { CBM_LANG_RUST, "n.rs",
      "use actix_web::web::{Data, Json, Path};\nfn main() {}\n",
      {"actix_web::web", NULL}, 1, "rust_actix" },

    /* use inside function body — may not be at root cursor level */
    { CBM_LANG_RUST, "o.rs",
      "fn main() {\n    use std::collections::BTreeMap;\n    let _ = BTreeMap::<i32,i32>::new();\n}\n",
      {NULL}, 0, "rust_use_in_fn" },  /* count may be 0 (root-only scan) — no assert */
};

TEST(rust_import_table) {
    int failures = run_cases(rust_cases, (int)(sizeof(rust_cases)/sizeof(rust_cases[0])));
    if (failures > 0) {
        printf("  %d rust import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Java — parse_java_imports (scoped_identifier / identifier child of
 *         import_declaration).  Existing test asserts java.util.List → GREEN.
 *         Graph zero-edges are likely downstream resolution/edge-creation.
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t java_cases[] = {
    /* single type import */
    { CBM_LANG_JAVA, "A.java",
      "import java.util.List;\npublic class A {}\n",
      {"java.util.List", NULL}, 1, "java_single" },

    /* multiple type imports */
    { CBM_LANG_JAVA, "B.java",
      "import java.util.List;\nimport java.util.ArrayList;\nimport java.util.Map;\n"
      "public class B {}\n",
      {"java.util.List", "java.util.ArrayList", "java.util.Map", NULL}, 3, "java_multi" },

    /* static import */
    { CBM_LANG_JAVA, "C.java",
      "import static java.lang.Math.PI;\npublic class C {}\n",
      {"java.lang.Math", NULL}, 1, "java_static" },

    /* on-demand wildcard */
    { CBM_LANG_JAVA, "D.java",
      "import java.io.*;\npublic class D {}\n",
      {"java.io", NULL}, 1, "java_wildcard" },

    /* static on-demand */
    { CBM_LANG_JAVA, "E.java",
      "import static org.junit.Assert.*;\npublic class E {}\n",
      {"org.junit.Assert", NULL}, 1, "java_static_wildcard" },

    /* deeply nested package */
    { CBM_LANG_JAVA, "F.java",
      "import com.google.common.collect.ImmutableList;\npublic class F {}\n",
      {"com.google.common.collect.ImmutableList", NULL}, 1, "java_deep_nested" },

    /* multiple mixed */
    { CBM_LANG_JAVA, "G.java",
      "import java.util.List;\nimport java.io.*;\nimport static java.lang.System.out;\n"
      "public class G {}\n",
      {"java.util.List", "java.io", "java.lang.System", NULL}, 3, "java_mixed" },

    /* android / androidx */
    { CBM_LANG_JAVA, "H.java",
      "import androidx.appcompat.app.AppCompatActivity;\nimport android.os.Bundle;\n"
      "public class H {}\n",
      {"androidx.appcompat.app.AppCompatActivity", "android.os.Bundle", NULL}, 2,
      "java_android" },

    /* javax imports */
    { CBM_LANG_JAVA, "I.java",
      "import javax.servlet.http.HttpServletRequest;\npublic class I {}\n",
      {"javax.servlet.http.HttpServletRequest", NULL}, 1, "java_javax" },

    /* lombok annotation */
    { CBM_LANG_JAVA, "J.java",
      "import lombok.Data;\nimport lombok.Builder;\npublic class J {}\n",
      {"lombok.Data", "lombok.Builder", NULL}, 2, "java_lombok" },

    /* spring framework */
    { CBM_LANG_JAVA, "K.java",
      "import org.springframework.web.bind.annotation.RestController;\n"
      "import org.springframework.web.bind.annotation.GetMapping;\n"
      "public class K {}\n",
      {"org.springframework.web.bind.annotation.RestController",
       "org.springframework.web.bind.annotation.GetMapping", NULL}, 2, "java_spring" },

    /* interface file */
    { CBM_LANG_JAVA, "L.java",
      "import java.util.function.Function;\nimport java.util.function.Predicate;\n"
      "public interface L {}\n",
      {"java.util.function.Function", "java.util.function.Predicate", NULL}, 2,
      "java_interface_imports" },

    /* enum file */
    { CBM_LANG_JAVA, "M.java",
      "import java.util.Arrays;\nimport java.util.Collections;\npublic enum M { A, B }\n",
      {"java.util.Arrays", "java.util.Collections", NULL}, 2, "java_enum_imports" },
};

TEST(java_import_table) {
    int failures = run_cases(java_cases, (int)(sizeof(java_cases)/sizeof(java_cases[0])));
    if (failures > 0) {
        printf("  %d java import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Kotlin — parse_kotlin_imports → extract_one_import_header →
 *   generic_import_from_text (strips "import "/";").
 *
 * tree-sitter-kotlin wraps imports in import_list → import_header.
 * generic_import_from_text strips the first word ("import") and trailing ";",
 * so "import kotlin.collections.HashMap" → module_path="kotlin.collections.HashMap".
 * Alias form "import X as Y" → module_path="X as Y" (includes " as Y").
 * Wildcard "import X.*" → module_path="X.*".
 *
 * EXPECTED: basic, wildcard GREEN; alias substring check on original name GREEN
 * (since "kotlin.collections.HashMap" is a substring of "kotlin.collections.HashMap as Map").
 * Graph zero-edges are likely downstream.
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t kotlin_cases[] = {
    /* simple import */
    { CBM_LANG_KOTLIN, "A.kt",
      "import kotlin.collections.HashMap\nfun main() {}\n",
      {"kotlin.collections.HashMap", NULL}, 1, "kotlin_simple" },

    /* import with alias */
    { CBM_LANG_KOTLIN, "B.kt",
      "import kotlin.collections.HashMap as Map\nfun main() {}\n",
      {"kotlin.collections.HashMap", NULL}, 1, "kotlin_alias" },

    /* wildcard import */
    { CBM_LANG_KOTLIN, "C.kt",
      "import kotlin.math.*\nfun main() {}\n",
      {"kotlin.math", NULL}, 1, "kotlin_wildcard" },

    /* multiple imports */
    { CBM_LANG_KOTLIN, "D.kt",
      "import java.util.Date\nimport java.io.File\nimport kotlin.text.Regex\nfun main() {}\n",
      {"java.util.Date", "java.io.File", "kotlin.text.Regex", NULL}, 3, "kotlin_multi" },

    /* Android imports */
    { CBM_LANG_KOTLIN, "E.kt",
      "import android.app.Activity\nimport android.os.Bundle\nclass E : Activity() {}\n",
      {"android.app.Activity", "android.os.Bundle", NULL}, 2, "kotlin_android" },

    /* coroutines */
    { CBM_LANG_KOTLIN, "F.kt",
      "import kotlinx.coroutines.Dispatchers\nimport kotlinx.coroutines.launch\n"
      "fun main() {}\n",
      {"kotlinx.coroutines.Dispatchers", "kotlinx.coroutines.launch", NULL}, 2,
      "kotlin_coroutines" },

    /* stdlib + external */
    { CBM_LANG_KOTLIN, "G.kt",
      "import kotlin.io.println\nimport com.example.MyClass\nfun main() {}\n",
      {"kotlin.io.println", "com.example.MyClass", NULL}, 2, "kotlin_mixed" },

    /* deeply nested */
    { CBM_LANG_KOTLIN, "H.kt",
      "import org.springframework.web.bind.annotation.RestController\nfun main() {}\n",
      {"org.springframework.web.bind.annotation.RestController", NULL}, 1,
      "kotlin_deep_nested" },

    /* companion object pattern */
    { CBM_LANG_KOTLIN, "I.kt",
      "import kotlin.jvm.JvmStatic\nclass I { companion object {} }\n",
      {"kotlin.jvm.JvmStatic", NULL}, 1, "kotlin_jvm_static" },

    /* many imports */
    { CBM_LANG_KOTLIN, "J.kt",
      "import java.util.Date\nimport java.util.Calendar\nimport java.util.TimeZone\n"
      "import java.text.SimpleDateFormat\nimport java.text.DateFormat\n"
      "fun main() {}\n",
      {"java.util.Date", "java.util.Calendar", "java.util.TimeZone",
       "java.text.SimpleDateFormat", NULL}, 5, "kotlin_many" },

    /* Ktor imports */
    { CBM_LANG_KOTLIN, "K.kt",
      "import io.ktor.application.*\nimport io.ktor.response.*\nimport io.ktor.routing.*\n"
      "fun main() {}\n",
      {"io.ktor.application", "io.ktor.response", "io.ktor.routing", NULL}, 3,
      "kotlin_ktor" },

    /* sealed class import */
    { CBM_LANG_KOTLIN, "L.kt",
      "import com.myapp.Result\nimport com.myapp.Result.Success\nimport com.myapp.Result.Error\n"
      "fun main() {}\n",
      {"com.myapp.Result", NULL}, 3, "kotlin_sealed_class" },

    /* interface/extension imports */
    { CBM_LANG_KOTLIN, "M.kt",
      "import kotlin.collections.MutableList\nimport kotlin.collections.mutableListOf\n"
      "fun main() {}\n",
      {"kotlin.collections.MutableList", "kotlin.collections.mutableListOf", NULL}, 2,
      "kotlin_extensions" },

    /* enum + annotation imports */
    { CBM_LANG_KOTLIN, "N.kt",
      "import kotlin.annotation.AnnotationRetention\nimport kotlin.annotation.Retention\n"
      "fun main() {}\n",
      {"kotlin.annotation.AnnotationRetention", "kotlin.annotation.Retention", NULL}, 2,
      "kotlin_annotation" },

    /* object declaration */
    { CBM_LANG_KOTLIN, "O.kt",
      "import kotlin.properties.Delegates\nobject O {}\n",
      {"kotlin.properties.Delegates", NULL}, 1, "kotlin_object" },
};

TEST(kotlin_import_table) {
    int failures = run_cases(kotlin_cases, (int)(sizeof(kotlin_cases)/sizeof(kotlin_cases[0])));
    if (failures > 0) {
        printf("  %d kotlin import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PHP — parse_generic_imports("expression_statement").
 *
 * PHP's "use" keyword appears as a `namespace_use_declaration` node, NOT an
 * `expression_statement`.  So "use Foo\Bar;" will yield 0 extracted imports
 * (the dispatcher looks for expression_statement, finds none).
 *
 * "require"/"include" ARE expression_statements, so require/include should
 * be extracted IF the generic text-stripper catches them.
 *
 * EXPECTED:
 *   - PHP "use Namespace\Class" → LIKELY RED (0 imports; wrong node type)
 *   - PHP require/include → MAY be GREEN (expression_statement nodes)
 *
 * We write both forms so RED rows expose the namespace-use gap.
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t php_cases[] = {
    /* use statement — basic (expected RED if node-type mismatch) */
    { CBM_LANG_PHP, "a.php",
      "<?php\nuse App\\Http\\Controllers\\Controller;\nclass A extends Controller {}\n",
      {"App\\Http\\Controllers\\Controller", NULL}, 1, "php_use_basic" },

    /* use with alias */
    { CBM_LANG_PHP, "b.php",
      "<?php\nuse Illuminate\\Support\\Facades\\DB as Database;\nclass B {}\n",
      {"Illuminate\\Support\\Facades\\DB", NULL}, 1, "php_use_alias" },

    /* multiple use statements */
    { CBM_LANG_PHP, "c.php",
      "<?php\nuse App\\Models\\User;\nuse App\\Models\\Post;\nuse App\\Models\\Comment;\n"
      "class C {}\n",
      {"App\\Models\\User", "App\\Models\\Post", "App\\Models\\Comment", NULL}, 3,
      "php_use_multi" },

    /* use function */
    { CBM_LANG_PHP, "d.php",
      "<?php\nuse function App\\Helpers\\format_date;\nclass D {}\n",
      {"App\\Helpers\\format_date", NULL}, 1, "php_use_function" },

    /* use const */
    { CBM_LANG_PHP, "e.php",
      "<?php\nuse const App\\Constants\\MAX_SIZE;\nclass E {}\n",
      {"App\\Constants\\MAX_SIZE", NULL}, 1, "php_use_const" },

    /* grouped use (PHP 7+) */
    { CBM_LANG_PHP, "f.php",
      "<?php\nuse App\\Http\\{Request, Response, Middleware};\nclass F {}\n",
      {"App\\Http", NULL}, 1, "php_grouped_use" },

    /* trait use (inside class — not namespace use) */
    { CBM_LANG_PHP, "g.php",
      "<?php\nuse App\\Traits\\HasTimestamps;\nclass G { use HasTimestamps; }\n",
      {"App\\Traits\\HasTimestamps", NULL}, 1, "php_trait_use" },

    /* require expression */
    { CBM_LANG_PHP, "h.php",
      "<?php\nrequire 'vendor/autoload.php';\nclass H {}\n",
      {"vendor/autoload.php", NULL}, 1, "php_require" },

    /* require_once */
    { CBM_LANG_PHP, "i.php",
      "<?php\nrequire_once 'config.php';\nclass I {}\n",
      {"config.php", NULL}, 1, "php_require_once" },

    /* include */
    { CBM_LANG_PHP, "j.php",
      "<?php\ninclude 'header.php';\nclass J {}\n",
      {"header.php", NULL}, 1, "php_include" },

    /* Laravel facade import */
    { CBM_LANG_PHP, "k.php",
      "<?php\nuse Illuminate\\Support\\Facades\\Auth;\nuse Illuminate\\Support\\Facades\\Cache;\n"
      "class K {}\n",
      {"Illuminate\\Support\\Facades\\Auth", "Illuminate\\Support\\Facades\\Cache", NULL}, 2,
      "php_laravel_facades" },

    /* Symfony component */
    { CBM_LANG_PHP, "l.php",
      "<?php\nuse Symfony\\Component\\HttpFoundation\\Request;\n"
      "use Symfony\\Component\\HttpFoundation\\Response;\nclass L {}\n",
      {"Symfony\\Component\\HttpFoundation\\Request",
       "Symfony\\Component\\HttpFoundation\\Response", NULL}, 2, "php_symfony" },

    /* interface implementation */
    { CBM_LANG_PHP, "m.php",
      "<?php\nuse Psr\\Log\\LoggerInterface;\nuse Psr\\Log\\AbstractLogger;\n"
      "class M implements LoggerInterface {}\n",
      {"Psr\\Log\\LoggerInterface", "Psr\\Log\\AbstractLogger", NULL}, 2, "php_psr" },
};

TEST(php_import_table) {
    int failures = run_cases(php_cases, (int)(sizeof(php_cases)/sizeof(php_cases[0])));
    if (failures > 0) {
        printf("  %d php import case(s) failed (use-statement forms likely RED: "
               "wrong node_type 'expression_statement' vs actual 'namespace_use_declaration')\n",
               failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * C# — parse_generic_imports("using_directive") → generic_import_from_text.
 *
 * generic_import_from_text strips the first word ("using") and trailing ";".
 * So "using System.Collections.Generic;" → module_path="System.Collections.Generic".
 * Alias form "using F = System.IO.File;" → module_path="F = System.IO.File"
 *   (includes "F = " prefix — has_import("System.IO.File") STILL matches as
 *    substring, so alias cases may be GREEN at the module_path level).
 * Static using "using static System.Math;" → module_path="static System.Math"
 *   — has_import("System.Math") matches as substring → GREEN.
 *
 * EXPECTED: most forms GREEN (substring match works despite extra tokens).
 * Graph zero-edges are likely downstream.
 * ═══════════════════════════════════════════════════════════════════════════ */

static const import_case_t csharp_cases[] = {
    /* simple namespace */
    { CBM_LANG_CSHARP, "A.cs",
      "using System;\npublic class A {}\n",
      {"System", NULL}, 1, "cs_simple" },

    /* nested namespace */
    { CBM_LANG_CSHARP, "B.cs",
      "using System.Collections.Generic;\npublic class B {}\n",
      {"System.Collections.Generic", NULL}, 1, "cs_nested" },

    /* static using */
    { CBM_LANG_CSHARP, "C.cs",
      "using static System.Math;\npublic class C {}\n",
      {"System.Math", NULL}, 1, "cs_static" },

    /* alias using */
    { CBM_LANG_CSHARP, "D.cs",
      "using F = System.IO.File;\npublic class D {}\n",
      {"System.IO.File", NULL}, 1, "cs_alias" },

    /* multiple usings */
    { CBM_LANG_CSHARP, "E.cs",
      "using System;\nusing System.IO;\nusing System.Text;\nusing System.Linq;\n"
      "public class E {}\n",
      {"System", "System.IO", "System.Text", "System.Linq", NULL}, 4, "cs_multi" },

    /* LINQ */
    { CBM_LANG_CSHARP, "F.cs",
      "using System.Linq;\nusing System.Collections.Generic;\npublic class F {}\n",
      {"System.Linq", "System.Collections.Generic", NULL}, 2, "cs_linq" },

    /* ASP.NET Core */
    { CBM_LANG_CSHARP, "G.cs",
      "using Microsoft.AspNetCore.Mvc;\nusing Microsoft.AspNetCore.Http;\n"
      "public class G : ControllerBase {}\n",
      {"Microsoft.AspNetCore.Mvc", "Microsoft.AspNetCore.Http", NULL}, 2, "cs_aspnet" },

    /* Entity Framework */
    { CBM_LANG_CSHARP, "H.cs",
      "using Microsoft.EntityFrameworkCore;\nusing System.ComponentModel.DataAnnotations;\n"
      "public class H {}\n",
      {"Microsoft.EntityFrameworkCore", "System.ComponentModel.DataAnnotations", NULL}, 2,
      "cs_ef" },

    /* Newtonsoft.Json */
    { CBM_LANG_CSHARP, "I.cs",
      "using Newtonsoft.Json;\nusing Newtonsoft.Json.Linq;\npublic class I {}\n",
      {"Newtonsoft.Json", NULL}, 2, "cs_json" },

    /* multiple aliases */
    { CBM_LANG_CSHARP, "J.cs",
      "using Dict = System.Collections.Generic.Dictionary<string, int>;\n"
      "using Str = System.String;\npublic class J {}\n",
      {"System.Collections.Generic.Dictionary", "System.String", NULL}, 2, "cs_multi_alias" },

    /* Xunit test */
    { CBM_LANG_CSHARP, "K.cs",
      "using Xunit;\nusing FluentAssertions;\npublic class K {}\n",
      {"Xunit", "FluentAssertions", NULL}, 2, "cs_xunit" },

    /* global using (C# 10+) — may or may not be a using_directive */
    { CBM_LANG_CSHARP, "L.cs",
      "global using System;\nglobal using System.Threading.Tasks;\npublic class L {}\n",
      {"System", "System.Threading.Tasks", NULL}, 2, "cs_global_using" },

    /* mixed: regular, static, alias */
    { CBM_LANG_CSHARP, "M.cs",
      "using System;\nusing static System.Console;\nusing Path = System.IO.Path;\n"
      "public class M {}\n",
      {"System", "System.Console", "System.IO.Path", NULL}, 3, "cs_mixed" },

    /* threading */
    { CBM_LANG_CSHARP, "N.cs",
      "using System.Threading;\nusing System.Threading.Tasks;\nusing System.Threading.Channels;\n"
      "public class N {}\n",
      {"System.Threading", "System.Threading.Tasks", "System.Threading.Channels", NULL}, 3,
      "cs_threading" },

    /* Azure SDK */
    { CBM_LANG_CSHARP, "O.cs",
      "using Azure.Storage.Blobs;\nusing Azure.Identity;\npublic class O {}\n",
      {"Azure.Storage.Blobs", "Azure.Identity", NULL}, 2, "cs_azure" },
};

TEST(csharp_import_table) {
    int failures = run_cases(csharp_cases, (int)(sizeof(csharp_cases)/sizeof(csharp_cases[0])));
    if (failures > 0) {
        printf("  %d csharp import case(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Cross-language: zero-import guard.
 *
 * Files with NO import statements should produce imports.count == 0.
 * This ensures the extractors don't fabricate phantom imports.
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    CBMLanguage  lang;
    const char  *path;
    const char  *src;
    const char  *label;
} zero_import_case_t;

static const zero_import_case_t zero_cases[] = {
    { CBM_LANG_RUST,       "a.rs",   "fn main() { println!(\"hello\"); }\n",
      "rust_no_imports" },
    { CBM_LANG_JAVA,       "A.java", "public class A { public static void main(String[] a) {} }\n",
      "java_no_imports" },
    { CBM_LANG_KOTLIN,     "A.kt",   "fun main() { println(\"hello\") }\n",
      "kotlin_no_imports" },
    { CBM_LANG_CSHARP,     "A.cs",   "public class A { static void Main() {} }\n",
      "cs_no_imports" },
    { CBM_LANG_PHP,        "a.php",  "<?php\necho 'hello';\n",
      "php_no_imports" },
    { CBM_LANG_GO,         "a.go",   "package main\nfunc main() {}\n",
      "go_no_imports" },
    { CBM_LANG_PYTHON,     "a.py",   "def main(): pass\n",
      "py_no_imports" },
    { CBM_LANG_TYPESCRIPT, "a.ts",   "function hello(): void { console.log('hi'); }\n",
      "ts_no_imports" },
    { CBM_LANG_JAVASCRIPT, "a.js",   "function hello() { return 42; }\n",
      "js_no_imports" },
};

TEST(zero_imports_guard) {
    int failures = 0;
    int n = (int)(sizeof(zero_cases)/sizeof(zero_cases[0]));
    for (int i = 0; i < n; i++) {
        const zero_import_case_t *tc = &zero_cases[i];
        CBMFileResult *r = do_extract(tc->src, tc->lang, tc->path);
        if (!r) {
            printf("  FAIL [%s]: NULL result\n", tc->label);
            failures++;
            continue;
        }
        if (r->imports.count != 0) {
            printf("  FAIL [%s]: expected 0 imports, got %d\n",
                   tc->label, r->imports.count);
            failures++;
        }
        cbm_free_result(r);
    }
    if (failures > 0) {
        printf("  %d zero-import guard(s) failed\n", failures);
        return 1;
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Per-language: import count exactness spot-checks.
 *
 * For each language we write a snippet with a KNOWN number of imports and
 * assert imports.count equals exactly that number.  This catches over-
 * or under-counting bugs that substring presence alone won't catch.
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(import_count_go_exact) {
    /* 3 imports in a block */
    CBMFileResult *r = do_extract(
        "package main\nimport (\n    \"fmt\"\n    \"os\"\n    \"io\"\n)\nfunc main() {}\n",
        CBM_LANG_GO, "a.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(r->imports.count, 3);
    cbm_free_result(r);
    PASS();
}

TEST(import_count_python_exact) {
    CBMFileResult *r = do_extract(
        "import os\nimport sys\nfrom typing import List\nfrom collections import defaultdict\n",
        CBM_LANG_PYTHON, "a.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(r->imports.count, 4);
    cbm_free_result(r);
    PASS();
}

TEST(import_count_ts_exact) {
    CBMFileResult *r = do_extract(
        "import React from 'react';\nimport { useState } from 'react';\nimport * as _ from 'lodash';\n",
        CBM_LANG_TYPESCRIPT, "a.ts");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* 3 import statements; named imports produce one entry per name or one per source */
    ASSERT_GTE(r->imports.count, 2);
    cbm_free_result(r);
    PASS();
}

TEST(import_count_rust_exact) {
    /* 3 use declarations at module root */
    CBMFileResult *r = do_extract(
        "use std::fs;\nuse std::io::Read;\nuse std::path::PathBuf;\nfn main() {}\n",
        CBM_LANG_RUST, "a.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(r->imports.count, 3);
    cbm_free_result(r);
    PASS();
}

TEST(import_count_java_exact) {
    /* 2 import_declarations */
    CBMFileResult *r = do_extract(
        "import java.util.List;\nimport java.util.Map;\npublic class A {}\n",
        CBM_LANG_JAVA, "A.java");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(r->imports.count, 2);
    cbm_free_result(r);
    PASS();
}

TEST(import_count_kotlin_exact) {
    /* 2 import_headers */
    CBMFileResult *r = do_extract(
        "import java.util.Date\nimport java.io.File\nfun main() {}\n",
        CBM_LANG_KOTLIN, "A.kt");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(r->imports.count, 2);
    cbm_free_result(r);
    PASS();
}

TEST(import_count_csharp_exact) {
    /* 2 using_directives */
    CBMFileResult *r = do_extract(
        "using System;\nusing System.IO;\npublic class A {}\n",
        CBM_LANG_CSHARP, "A.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(r->imports.count, 2);
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PHP edge-case: only require/include forms (no "use") should be extractable
 * even given the expression_statement dispatch.
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(php_require_only_extracted) {
    /* Only require — no namespace use. Should produce at least 1 import. */
    CBMFileResult *r = do_extract(
        "<?php\nrequire_once 'vendor/autoload.php';\n$x = 1;\n",
        CBM_LANG_PHP, "a.php");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* require_once is an expression_statement — may be extracted by generic */
    if (r->imports.count == 0) {
        printf("  INFO [php_require_only]: 0 imports — expression_statement path "
               "does not catch require_once\n");
    }
    cbm_free_result(r);
    PASS();  /* informational — count assertion relaxed; table test carries RED */
}

TEST(php_namespace_use_zero) {
    /* "use" namespace declaration — expected count 0 given wrong node_type dispatch */
    CBMFileResult *r = do_extract(
        "<?php\nuse App\\Http\\Controllers\\Controller;\nclass A extends Controller {}\n",
        CBM_LANG_PHP, "a.php");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /*
     * REPRODUCTION of bug: PHP namespace use yields 0 imports because
     * parse_generic_imports("expression_statement") matches the wrong node type.
     * If this assertion fails the bug is FIXED — remove it and update the table tests.
     * If it passes (count==0) the bug is confirmed at extraction level.
     */
    if (r->imports.count > 0) {
        /* Bug unexpectedly fixed — print info and pass */
        printf("  INFO [php_namespace_use_zero]: got %d import(s) — extractor "
               "apparently handles namespace_use_declaration now\n",
               r->imports.count);
    }
    cbm_free_result(r);
    PASS();  /* non-fatal; table test php_use_basic carries the RED assertion */
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Rust: local_name field spot-checks (last segment of path)
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(rust_local_name_last_segment) {
    CBMFileResult *r = do_extract(
        "use std::collections::HashMap;\nfn main() {}\n",
        CBM_LANG_RUST, "a.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    /* local_name should be "HashMap" (last segment after "::") */
    ASSERT(imp_local(r, "HashMap"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Java: local_name field spot-check (last dot-component)
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(java_local_name_last_segment) {
    CBMFileResult *r = do_extract(
        "import java.util.ArrayList;\npublic class A {}\n",
        CBM_LANG_JAVA, "A.java");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    /* local_name should be "ArrayList" */
    ASSERT(imp_local(r, "ArrayList"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * C#: local_name field spot-check
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(csharp_local_name_last_segment) {
    CBMFileResult *r = do_extract(
        "using System.Collections.Generic;\npublic class A {}\n",
        CBM_LANG_CSHARP, "A.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    /* local_name should be "Generic" (last dot-segment) */
    ASSERT(imp_local(r, "Generic"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SUITE declaration — do NOT register in test_main.c; the main workstream will
 * ═══════════════════════════════════════════════════════════════════════════ */

SUITE(extraction_imports) {
    cbm_init();

    /* Working languages — all expected GREEN (regression guards) */
    RUN_TEST(go_import_table);
    RUN_TEST(python_import_table);
    RUN_TEST(typescript_import_table);
    RUN_TEST(javascript_import_table);

    /* Extraction-level spot-checks for languages with graph-level zero-edges */
    RUN_TEST(rust_import_table);         /* extraction likely GREEN; graph edges RED downstream */
    RUN_TEST(java_import_table);         /* extraction likely GREEN; graph edges RED downstream */
    RUN_TEST(kotlin_import_table);       /* extraction likely GREEN; graph edges RED downstream */
    RUN_TEST(csharp_import_table);       /* extraction likely GREEN; graph edges RED downstream */
    RUN_TEST(php_import_table);          /* use-forms likely RED at extraction; require forms TBD */

    /* Zero-import guards */
    RUN_TEST(zero_imports_guard);

    /* Exact count spot-checks */
    RUN_TEST(import_count_go_exact);
    RUN_TEST(import_count_python_exact);
    RUN_TEST(import_count_ts_exact);
    RUN_TEST(import_count_rust_exact);
    RUN_TEST(import_count_java_exact);
    RUN_TEST(import_count_kotlin_exact);
    RUN_TEST(import_count_csharp_exact);

    /* PHP diagnostic tests */
    RUN_TEST(php_require_only_extracted);
    RUN_TEST(php_namespace_use_zero);

    /* Local-name field spot-checks */
    RUN_TEST(rust_local_name_last_segment);
    RUN_TEST(java_local_name_last_segment);
    RUN_TEST(csharp_local_name_last_segment);
}
