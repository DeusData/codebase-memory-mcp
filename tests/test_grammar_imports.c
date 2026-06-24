/*
 * test_grammar_imports.c — Per-grammar IMPORTS EXTRACTION breadth.
 *
 * For every grammar cbm_extract_imports() handles, assert cbm_extract_file()
 * captures its import/include/use/require statements (imports.count >= N).
 * This is the EXTRACTION-level import contract: it uses an explicit language
 * (no discover, no pkgmap), so it is reliable in-unit for ALL import-capable
 * grammars — the graph IMPORTS-edge (which needs module resolution) is covered
 * for relative imports by test_lang_contract.c and at real scale by the scale
 * tier. No skips: a grammar that fails to extract its imports is a hard FAILURE
 * reproducing the bug (gap_note documents the already-root-caused ones).
 */
#include "test_framework.h"
#include "cbm.h"

#include <string.h>

typedef struct {
    const char *name;
    CBMLanguage lang;
    const char *filename;
    const char *content;
    int expected; /* imports.count must be >= this */
    const char *gap_note;
} ImportCase;

static const ImportCase IMPORT_CASES[] = {
    {"astro", CBM_LANG_ASTRO, "fixture.astro",
     "<script>\nimport confetti from \"canvas-confetti\";\nimport dayjs from \"dayjs\";\n\nconfetti();\nconsole.log(dayjs().format());\n</script>\n<div>hi</div>\n",
     2, NULL},
    {"bash", CBM_LANG_BASH, "a.sh", "#!/usr/bin/env bash\nsource ./lib/util.sh\n. ./lib/other.sh\n", 2,
     NULL},
    {"c", CBM_LANG_C, "imports_fixture.c",
     "#include <stdio.h>\n#include \"mymodule.h\"\n\nint main(void) {\n    return 0;\n}\n", 2, NULL},
    {"cpp", CBM_LANG_CPP, "main.cpp",
     "#include <iostream>\n#include \"widget.h\"\n\nint main() {\n    std::cout << \"hi\\n\";\n    return 0;\n}\n",
     2, NULL},
    {"csharp", CBM_LANG_CSHARP, "Imports.cs",
     "using System;\nusing static System.Math;\nusing Json = System.Text.Json;\n\nnamespace Demo;\n\nclass Program\n{\n    static void Main() => Console.WriteLine(Sqrt(4.0));\n}\n",
     3, NULL},
    {"css", CBM_LANG_CSS, "styles.css",
     "@import \"reset.css\";\n@import url(\"theme.css\");\n\nbody {\n  margin: 0;\n}\n", 2, NULL},
    {"dart", CBM_LANG_DART, "imports.dart",
     "import 'dart:async';\nimport 'package:meta/meta.dart';\n\nvoid main() {}\n", 2,
     "extract_imports.c SWIFT/DART case matches 'import_declaration' but tree-sitter-dart emits 'import_or_export' -> 0 imports"},
    {"elixir", CBM_LANG_ELIXIR, "imports_fixture.ex",
     "import Enum\nalias MyApp.Accounts.User\nrequire Logger\n", 3, NULL},
    {"erlang", CBM_LANG_ERLANG, "greeter.erl",
     "-module(greeter).\n-import(lists, [foldl/3]).\n-include_lib(\"kernel/include/file.hrl\").\n\nsum(Ns) -> foldl(fun(X, A) -> X + A end, 0, Ns).\n",
     1, NULL},
    {"form", CBM_LANG_FORM, "imports.frm", "#include \"procedures.prc\"\n#include \"tables.h\"\n", 2, NULL},
    {"go", CBM_LANG_GO, "imports_fixture.go",
     "package main\n\nimport (\n\t\"fmt\"\n\t\"os\"\n\tmrand \"math/rand\"\n)\n\nfunc main() {\n\tfmt.Fprintln(os.Stdout, mrand.Int())\n}\n",
     3, NULL},
    {"groovy", CBM_LANG_GROOVY, "a.groovy",
     "import java.util.List\nimport groovy.transform.CompileStatic\n\nclass Greeter {\n    String greet() { \"hi\" }\n}\n",
     2, NULL},
    {"haskell", CBM_LANG_HASKELL, "a.hs",
     "module Main where\n\nimport Data.List (sort)\nimport qualified Data.Map as Map\n\nmain :: IO ()\nmain = print (sort [3, 1, 2])\n",
     2,
     "imports nested under an 'imports' container node; parse_generic_imports scans only root children -> 0 (needs parse_haskell_imports, like Kotlin)"},
    {"html", CBM_LANG_HTML, "fixture_imports.html",
     "<!DOCTYPE html>\n<html lang=\"en\">\n  <head>\n    <meta charset=\"utf-8\" />\n    <title>Import Fixture</title>\n    <script type=\"module\">\n      import App from \"./app.js\";\n      import { render } from \"./render.js\";\n\n      render(App);\n    </script>\n  </head>\n  <body></body>\n</html>\n",
     2, NULL},
    {"java", CBM_LANG_JAVA, "Fixture.java",
     "import java.util.List;\nimport java.util.Map;\n\nclass Fixture {\n    List<String> names;\n    Map<String, Integer> counts;\n}\n",
     2, NULL},
    {"javascript", CBM_LANG_JAVASCRIPT, "imports_fixture.js",
     "import path from \"node:path\";\nimport { readFile, writeFile } from \"node:fs/promises\";\n\nexport async function copyConfig(src) {\n  const dst = path.join(\"/tmp\", \"config.json\");\n  const data = await readFile(src, \"utf8\");\n  await writeFile(dst, data);\n  return dst;\n}\n",
     3, NULL},
    {"kotlin", CBM_LANG_KOTLIN, "fixture.kt",
     "package com.example.app\n\nimport kotlin.collections.List\nimport java.util.UUID\nimport kotlinx.coroutines.runBlocking\n",
     3, NULL},
    {"lean", CBM_LANG_LEAN, "imports.lean",
     "import Std.Data.List.Basic\nimport Mathlib.Tactic\n\ndef hello : String := \"world\"\n", 2, NULL},
    {"lua", CBM_LANG_LUA, "a.lua",
     "local socket = require(\"socket\")\nlocal json = require(\"dkjson\")\n\nlocal function connect()\n  return socket, json\nend\n\nreturn connect\n",
     2, NULL},
    {"magma", CBM_LANG_MAGMA, "a.magma",
     "load \"mylib.m\";\nload \"helpers.m\";\n\nfunction foo()\n  return 1;\nend function;\n", 2, NULL},
    {"objc", CBM_LANG_OBJC, "fixture_imports.m",
     "#import <Foundation/Foundation.h>\n#import \"AppModel.h\"\n", 2, NULL},
    {"ocaml", CBM_LANG_OCAML, "a.ml",
     "open List\nopen String\n\nlet greet name = String.cat \"Hi \" name\n", 2, NULL},
    {"perl", CBM_LANG_PERL, "imports.pl",
     "use strict;\nuse warnings;\nuse Data::Dumper;\n\nsub run { print Dumper([1, 2, 3]); }\n", 3, NULL},
    {"php", CBM_LANG_PHP, "a.php",
     "<?php\nrequire_once 'vendor/autoload.php';\ninclude 'config.php';\n", 2, NULL},
    {"python", CBM_LANG_PYTHON, "fixture_imports.py",
     "import os\nfrom typing import List, Optional\n\n\ndef widths(items: List[str], pad: Optional[int]) -> List[int]:\n    return [len(s) + (pad or 0) for s in items if os.path.exists(s)]\n",
     3, NULL},
    {"r", CBM_LANG_R, "imports_fixture.R", "library(jsonlite)\nrequire(stats)\nsource(\"utils.R\")\n", 3,
     NULL},
    {"ruby", CBM_LANG_RUBY, "a.rb", "require \"json\"\nrequire_relative \"helper\"\n", 2, NULL},
    {"rust", CBM_LANG_RUST, "imports.rs",
     "use std::collections::HashMap;\nuse std::fmt::{self, Display};\nuse serde::Serialize;\n\nfn main() {\n    let _m: HashMap<String, i32> = HashMap::new();\n}\n",
     3, NULL},
    {"scala", CBM_LANG_SCALA, "a.scala",
     "package example\n\nimport scala.collection.mutable.ListBuffer\nimport java.time.Instant\n\nobject Main {\n  def run(): Unit = {\n    val buf = ListBuffer[Instant]()\n    buf += Instant.now()\n  }\n}\n",
     2, NULL},
    {"scss", CBM_LANG_SCSS, "imports.scss",
     "@import \"variables\";\n@import \"mixins\";\n\n.button {\n  color: red;\n}\n", 2, NULL},
    {"svelte", CBM_LANG_SVELTE, "a.svelte",
     "<script>\n  import { onMount } from 'svelte';\n  import App from './App.svelte';\n\n  onMount(() => {});\n</script>\n\n<App />\n",
     2, NULL},
    {"swift", CBM_LANG_SWIFT, "a.swift", "import Foundation\nimport SwiftUI\n", 2, NULL},
    {"tsx", CBM_LANG_TSX, "a.tsx",
     "import React from \"react\";\nimport { useState, useEffect } from \"react\";\nimport * as styles from \"./Button.module.css\";\n\nexport function Button(): React.ReactElement {\n  const [count, setCount] = useState(0);\n  useEffect(() => {}, []);\n  return <button className={styles.root} onClick={() => setCount(count + 1)}>{count}</button>;\n}\n",
     4, NULL},
    {"typescript", CBM_LANG_TYPESCRIPT, "imports_fixture.ts",
     "import express from \"express\";\nimport * as path from \"path\";\nimport { useState, useEffect } from \"react\";\n\nconst app = express();\nconst dir: string = path.join(\".\");\nconsole.log(app, dir, useState, useEffect);\n",
     4, NULL},
    {"vue", CBM_LANG_VUE, "component.vue",
     "<script setup>\nimport { ref } from 'vue'\nimport HelloWorld from './HelloWorld.vue'\n\nconst msg = ref('hello')\n</script>\n\n<template>\n  <HelloWorld :msg=\"msg\" />\n</template>\n",
     2, NULL},
    {"wolfram", CBM_LANG_WOLFRAM, "main.wl",
     "Needs[\"MyPackage`Utils`\"]\n<< \"helpers.wl\"\n\nmain[x_] := process[x] + 1\n", 2, NULL},
    {"zig", CBM_LANG_ZIG, "imports.zig",
     "const std = @import(\"std\");\nconst builtin = @import(\"builtin\");\n\npub fn main() void {}\n", 2,
     "@import is nested in variable_declaration; parse_generic_imports scans only root children for builtin_function -> 0"},
};

TEST(grammar_imports_extracted) {
    int n = (int)(sizeof(IMPORT_CASES) / sizeof(IMPORT_CASES[0]));
    int failures = 0;
    for (int i = 0; i < n; i++) {
        const ImportCase *c = &IMPORT_CASES[i];
        CBMFileResult *r = cbm_extract_file(c->content, (int)strlen(c->content), c->lang, "imp",
                                            c->filename, 0, NULL, NULL);
        if (!r) {
            fprintf(stderr, "  [IMPORTS] FAIL %-12s extract returned NULL\n", c->name);
            failures++;
            continue;
        }
        int got = r->imports.count;
        cbm_free_result(r);
        if (got < c->expected) {
            fprintf(stderr, "  [IMPORTS] FAIL %-12s imports=%d expected>=%d%s%s\n", c->name, got,
                    c->expected, c->gap_note ? " — " : "", c->gap_note ? c->gap_note : "");
            failures++;
        }
    }
    fprintf(stderr, "  [IMPORTS] %d import-capable grammars: %d FAILURES (each = a grammar whose "
                    "imports are not extracted)\n",
            n, failures);
    ASSERT_EQ(failures, 0);
    PASS();
}

void suite_grammar_imports(void) {
    RUN_TEST(grammar_imports_extracted);
}
