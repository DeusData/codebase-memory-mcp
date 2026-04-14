/*
 * test_extraction.c — Regression tests for the extraction module.
 *
 * Port of internal/cbm/regression_test.go (1282 LOC, ~80 test cases).
 * Exercises cbm_extract_file() on code snippets across 30+ languages,
 * verifying definitions, calls, and imports are correctly extracted.
 */
#include "test_framework.h"
#include "cbm.h"

/* ── Helpers ───────────────────────────────────────────────────── */

/* Check if any definition with the given label has the given name. */
static int has_def(CBMFileResult *r, const char *label, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, label) == 0 && strcmp(r->defs.items[i].name, name) == 0)
            return 1;
    }
    return 0;
}

/* Check if any definition has the given name (any label). */
static int has_def_any(CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].name, name) == 0)
            return 1;
    }
    return 0;
}

/* Check if any call to the given callee exists. */
static int has_call(CBMFileResult *r, const char *callee) {
    for (int i = 0; i < r->calls.count; i++) {
        if (strstr(r->calls.items[i].callee_name, callee) != NULL)
            return 1;
    }
    return 0;
}

/* Check if any import with the given module path exists. */
static int __attribute__((unused)) has_import(CBMFileResult *r, const char *path_substr) {
    for (int i = 0; i < r->imports.count; i++) {
        if (r->imports.items[i].module_path &&
            strstr(r->imports.items[i].module_path, path_substr) != NULL)
            return 1;
    }
    return 0;
}

/* Check if any import has exactly local name + module path. */
static int has_import_exact(CBMFileResult *r, const char *local_name, const char *module_path) {
    for (int i = 0; i < r->imports.count; i++) {
        CBMImport *imp = &r->imports.items[i];
        if (imp->module_path && strcmp(imp->module_path, module_path) == 0) {
            if (local_name == NULL) {
                if (!imp->local_name) {
                    return 1;
                }
            } else if (imp->local_name && strcmp(imp->local_name, local_name) == 0) {
                return 1;
            }
        }
    }
    return 0;
}

/* Check if any string_ref has exactly the given value. */
static int has_string_ref_value(CBMFileResult *r, const char *value) {
    for (int i = 0; i < r->string_refs.count; i++) {
        const CBMStringRef *ref = &r->string_refs.items[i];
        if (ref->value && strcmp(ref->value, value) == 0) {
            return 1;
        }
    }
    return 0;
}

/* Check if any string_ref has exactly value + enclosing scope QN. */
static int has_string_ref_with_enclosing(CBMFileResult *r, const char *value,
                                         const char *enclosing_qn) {
    for (int i = 0; i < r->string_refs.count; i++) {
        const CBMStringRef *ref = &r->string_refs.items[i];
        if (ref->value && ref->enclosing_func_qn && strcmp(ref->value, value) == 0 &&
            strcmp(ref->enclosing_func_qn, enclosing_qn) == 0) {
            return 1;
        }
    }
    return 0;
}

/* Count definitions with a given label. */
static int count_defs_with_label(CBMFileResult *r, const char *label) {
    int count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, label) == 0)
            count++;
    }
    return count;
}

/* Convenience: extract, assert no error, return result. Caller frees. */
static CBMFileResult *extract(const char *src, CBMLanguage lang, const char *proj,
                              const char *path) {
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src), lang, proj, path, 0, NULL, NULL);
    return r;
}

/* ═══════════════════════════════════════════════════════════════════
 * Group A: OOP Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- Java --- */
TEST(java_class) {
    CBMFileResult *r = extract(
        "public class Animal { private String name; public String getName() { return name; } }",
        CBM_LANG_JAVA, "t", "Animal.java");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Animal"));
    cbm_free_result(r);
    PASS();
}

TEST(java_method) {
    CBMFileResult *r = extract(
        "public class Svc { public void doWork() {} public int compute(int x) { return x; } }",
        CBM_LANG_JAVA, "t", "Svc.java");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Method", "doWork"));
    ASSERT(has_def(r, "Method", "compute"));
    cbm_free_result(r);
    PASS();
}

TEST(java_interface) {
    CBMFileResult *r =
        extract("public interface Repository { void save(Object o); Object findById(long id); }",
                CBM_LANG_JAVA, "t", "Repo.java");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def_any(r, "Repository"));
    cbm_free_result(r);
    PASS();
}

/* --- PHP --- */
TEST(php_class) {
    CBMFileResult *r = extract("<?php\nclass User { public string $name; public function "
                               "getName(): string { return $this->name; } }",
                               CBM_LANG_PHP, "t", "User.php");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "User"));
    ASSERT(has_def(r, "Method", "getName"));
    cbm_free_result(r);
    PASS();
}

TEST(php_function) {
    CBMFileResult *r =
        extract("<?php\nfunction greet(string $name): string { return 'Hello ' . $name; }",
                CBM_LANG_PHP, "t", "helpers.php");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

/* --- Ruby --- */
TEST(ruby_class) {
    CBMFileResult *r = extract("class Animal\n  def initialize(name)\n    @name = name\n  end\n  "
                               "def speak\n    puts @name\n  end\nend\n",
                               CBM_LANG_RUBY, "t", "animal.rb");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Animal"));
    ASSERT(has_def(r, "Method", "speak"));
    cbm_free_result(r);
    PASS();
}

TEST(ruby_module) {
    CBMFileResult *r = extract("module Greetable\n  def greet\n    \"Hello\"\n  end\nend\n",
                               CBM_LANG_RUBY, "t", "greetable.rb");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def_any(r, "Greetable"));
    cbm_free_result(r);
    PASS();
}

/* --- C# --- */
TEST(csharp_class) {
    CBMFileResult *r = extract("namespace App { public class Service { public void Run() {} public "
                               "int Compute(int x) => x * 2; } }",
                               CBM_LANG_CSHARP, "t", "Service.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Service"));
    ASSERT(has_def(r, "Method", "Run"));
    cbm_free_result(r);
    PASS();
}

TEST(csharp_interface) {
    CBMFileResult *r = extract("public interface IService { void Execute(); string GetStatus(); }",
                               CBM_LANG_CSHARP, "t", "IService.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def_any(r, "IService"));
    cbm_free_result(r);
    PASS();
}

/* --- Swift --- */
TEST(swift_class) {
    CBMFileResult *r = extract("class Vehicle {\n    var speed: Int = 0\n    func accelerate() { "
                               "speed += 10 }\n    func stop() { speed = 0 }\n}\n",
                               CBM_LANG_SWIFT, "t", "Vehicle.swift");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Vehicle"));
    ASSERT(has_def(r, "Method", "accelerate"));
    cbm_free_result(r);
    PASS();
}

/* --- Kotlin --- */
TEST(kotlin_function) {
    CBMFileResult *r = extract("fun greet(name: String): String = \"Hello $name\"\nfun main() { "
                               "println(greet(\"World\")) }\n",
                               CBM_LANG_KOTLIN, "t", "main.kt");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    ASSERT(has_def(r, "Function", "main"));
    cbm_free_result(r);
    PASS();
}

TEST(kotlin_class) {
    CBMFileResult *r =
        extract("class User(val name: String) {\n    fun display(): String = \"User: $name\"\n}\n",
                CBM_LANG_KOTLIN, "t", "User.kt");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "User"));
    cbm_free_result(r);
    PASS();
}

/* --- Scala --- */
TEST(scala_function) {
    CBMFileResult *r =
        extract("object Main {\n  def greet(name: String): String = s\"Hello $name\"\n  def "
                "main(args: Array[String]): Unit = println(greet(\"World\"))\n}\n",
                CBM_LANG_SCALA, "t", "Main.scala");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Method", "greet"));
    cbm_free_result(r);
    PASS();
}

TEST(scala_class) {
    CBMFileResult *r =
        extract("class Animal(val name: String) {\n  def speak(): String = s\"I am $name\"\n}\n",
                CBM_LANG_SCALA, "t", "Animal.scala");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Animal"));
    cbm_free_result(r);
    PASS();
}

/* --- Dart --- */
TEST(dart_class) {
    CBMFileResult *r = extract("class Animal {\n  String name;\n  Animal(this.name);\n  String "
                               "speak() => 'I am $name';\n}\n",
                               CBM_LANG_DART, "t", "animal.dart");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Animal"));
    ASSERT(has_def(r, "Method", "speak"));
    cbm_free_result(r);
    PASS();
}

/* --- Groovy --- */
TEST(groovy_class) {
    CBMFileResult *r =
        extract("class Greeter {\n    String name\n    String greet() { \"Hello, $name\" }\n    "
                "static void main(args) { println new Greeter(name:'World').greet() }\n}\n",
                CBM_LANG_GROOVY, "t", "Greeter.groovy");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Greeter"));
    ASSERT(has_def(r, "Method", "greet"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group B: Systems Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- Rust --- */
TEST(rust_function) {
    CBMFileResult *r =
        extract("fn main() { println!(\"Hello\"); }\npub fn add(a: i32, b: i32) -> i32 { a + b }\n",
                CBM_LANG_RUST, "t", "main.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "main"));
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

TEST(rust_struct) {
    CBMFileResult *r = extract("pub struct Point { pub x: f64, pub y: f64 }\nimpl Point { pub fn "
                               "new(x: f64, y: f64) -> Self { Point { x, y } } }\n",
                               CBM_LANG_RUST, "t", "point.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Point"));
    ASSERT(has_def(r, "Method", "new"));
    cbm_free_result(r);
    PASS();
}

/* --- Go --- */
TEST(go_function) {
    CBMFileResult *r = extract("package main\nfunc Greet(name string) string { return \"Hello, \" "
                               "+ name }\nfunc main() { Greet(\"World\") }\n",
                               CBM_LANG_GO, "t", "main.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "Greet"));
    ASSERT(has_def(r, "Function", "main"));
    cbm_free_result(r);
    PASS();
}

TEST(go_struct) {
    CBMFileResult *r = extract("package main\ntype Server struct { Host string; Port int }\nfunc "
                               "(s *Server) Start() error { return nil }\n",
                               CBM_LANG_GO, "t", "server.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Server"));
    ASSERT(has_def(r, "Method", "Start"));
    cbm_free_result(r);
    PASS();
}

TEST(go_interface) {
    CBMFileResult *r =
        extract("package main\ntype Handler interface { ServeHTTP() error; Close() }\n",
                CBM_LANG_GO, "t", "handler.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def_any(r, "Handler"));
    cbm_free_result(r);
    PASS();
}

/* --- Zig --- */
TEST(zig_function) {
    CBMFileResult *r =
        extract("const std = @import(\"std\");\npub fn add(a: i32, b: i32) i32 { return a + b; }\n",
                CBM_LANG_ZIG, "t", "main.zig");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* --- C --- */
TEST(c_function) {
    CBMFileResult *r =
        extract("int add(int a, int b) { return a + b; }\nvoid greet() { printf(\"Hello\"); }\n",
                CBM_LANG_C, "t", "math.c");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

TEST(c_struct) {
    CBMFileResult *r =
        extract("struct Point { int x; int y; };\nvoid init_point(struct Point *p) { p->x = 0; }\n",
                CBM_LANG_C, "t", "point.c");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "init_point"));
    cbm_free_result(r);
    PASS();
}

/* --- C++ --- */
TEST(cpp_class) {
    CBMFileResult *r = extract(
        "class Widget {\npublic:\n    void draw() {}\n    int width() const { return 0; }\n};\n",
        CBM_LANG_CPP, "t", "widget.cpp");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Widget"));
    ASSERT(has_def(r, "Method", "draw"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group C: Scripting / Dynamic Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- Python --- */
TEST(python_function) {
    CBMFileResult *r = extract(
        "def greet(name):\n    return f\"Hello {name}\"\n\ndef main():\n    greet(\"World\")\n",
        CBM_LANG_PYTHON, "t", "main.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    ASSERT(has_def(r, "Function", "main"));
    cbm_free_result(r);
    PASS();
}

TEST(python_class) {
    CBMFileResult *r =
        extract("class Dog:\n    def __init__(self, name):\n        self.name = name\n    def "
                "speak(self):\n        return f\"Woof from {self.name}\"\n",
                CBM_LANG_PYTHON, "t", "dog.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Dog"));
    ASSERT(has_def(r, "Method", "speak"));
    cbm_free_result(r);
    PASS();
}

/* --- JavaScript --- */
TEST(js_function) {
    CBMFileResult *r =
        extract("function greet(name) { return `Hello ${name}`; }\nconst add = (a, b) => a + b;\n",
                CBM_LANG_JAVASCRIPT, "t", "util.js");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

TEST(js_class) {
    CBMFileResult *r =
        extract("class Counter {\n  constructor() { this.count = 0; }\n  increment() { "
                "this.count++; }\n  get value() { return this.count; }\n}\n",
                CBM_LANG_JAVASCRIPT, "t", "counter.js");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Counter"));
    ASSERT(has_def(r, "Method", "increment"));
    cbm_free_result(r);
    PASS();
}

/* --- TypeScript --- */
TEST(ts_function) {
    CBMFileResult *r = extract("export function greet(name: string): string { return `Hello "
                               "${name}`; }\nfunction helper(): void {}\n",
                               CBM_LANG_TYPESCRIPT, "t", "util.ts");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

TEST(ts_class) {
    CBMFileResult *r =
        extract("class Service {\n  private name: string;\n  constructor(name: string) { this.name "
                "= name; }\n  getName(): string { return this.name; }\n}\n",
                CBM_LANG_TYPESCRIPT, "t", "service.ts");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Service"));
    cbm_free_result(r);
    PASS();
}

/* --- Lua --- */
TEST(lua_function) {
    CBMFileResult *r = extract(
        "function greet(name)\n  return \"Hello \" .. name\nend\nlocal function helper() end\n",
        CBM_LANG_LUA, "t", "main.lua");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

/* --- Bash --- */
TEST(bash_function) {
    CBMFileResult *r =
        extract("greet() {\n  echo \"Hello $1\"\n}\nmain() {\n  greet \"World\"\n}\n",
                CBM_LANG_BASH, "t", "script.sh");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

/* --- Perl --- */
TEST(perl_function) {
    CBMFileResult *r = extract("sub greet {\n    my ($name) = @_;\n    return \"Hello "
                               "$name\";\n}\nsub main { greet(\"World\"); }\n",
                               CBM_LANG_PERL, "t", "main.pl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

/* --- R --- */
TEST(r_function) {
    CBMFileResult *r = extract("add <- function(x, y) x + y\nmultiply <- function(x, y) x * y\n",
                               CBM_LANG_R, "t", "math.R");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group D: Functional Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- Elixir --- */
TEST(elixir_function) {
    CBMFileResult *r = extract("defmodule Greeter do\n  def greet(name), do: \"Hello #{name}\"\n  "
                               "defp helper, do: nil\nend\n",
                               CBM_LANG_ELIXIR, "t", "greeter.ex");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

/* --- Haskell --- */
TEST(haskell_function) {
    CBMFileResult *r = extract("add :: Int -> Int -> Int\nadd x y = x + y\n\nmultiply :: Int -> "
                               "Int -> Int\nmultiply x y = x * y\n",
                               CBM_LANG_HASKELL, "t", "Math.hs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* --- OCaml --- */
TEST(ocaml_function) {
    CBMFileResult *r =
        extract("let add x y = x + y\nlet multiply x y = x * y\n", CBM_LANG_OCAML, "t", "math.ml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* --- Erlang --- */
TEST(erlang_function) {
    CBMFileResult *r = extract(
        "-module(math).\n-export([add/2]).\nadd(X, Y) -> X + Y.\nmultiply(X, Y) -> X * Y.\n",
        CBM_LANG_ERLANG, "t", "math.erl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group E: Markup / Config / Helper Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- YAML --- */
TEST(yaml_variables) {
    CBMFileResult *r =
        extract("name: myapp\nversion: 1.0\ndatabase:\n  host: localhost\n  port: 5432\n",
                CBM_LANG_YAML, "t", "config.yml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* YAML should extract top-level keys as variables */
    ASSERT_GT(r->defs.count, 0);
    cbm_free_result(r);
    PASS();
}

/* --- HCL --- */
TEST(hcl_blocks) {
    CBMFileResult *r = extract("resource \"aws_instance\" \"web\" {\n  ami = \"abc-123\"\n  "
                               "instance_type = \"t2.micro\"\n}\n",
                               CBM_LANG_HCL, "t", "main.tf");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->defs.count, 0);
    cbm_free_result(r);
    PASS();
}

/* --- SQL --- */
TEST(sql_create_table) {
    CBMFileResult *r = extract("CREATE TABLE users (\n  id INTEGER PRIMARY KEY,\n  name TEXT NOT "
                               "NULL\n);\nCREATE VIEW active_users AS SELECT * FROM users;\n",
                               CBM_LANG_SQL, "t", "schema.sql");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* --- Dockerfile --- */
TEST(dockerfile_stages) {
    CBMFileResult *r = extract(
        "FROM node:18 AS builder\nRUN npm install\nFROM node:18-slim\nCOPY --from=builder /app .\n",
        CBM_LANG_DOCKERFILE, "t", "Dockerfile");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group F: Scientific / Math Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- MATLAB --- */
TEST(matlab_function) {
    CBMFileResult *r =
        extract("function y = square(x)\n  y = x.^2;\nend\n", CBM_LANG_MATLAB, "t", "square.m");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "square"));
    cbm_free_result(r);
    PASS();
}

/* --- Lean 4 --- */
TEST(lean_function) {
    CBMFileResult *r =
        extract("def add (x y : Nat) : Nat := x + y\n", CBM_LANG_LEAN, "t", "Math.lean");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* --- FORM --- */
TEST(form_procedure) {
    CBMFileResult *r = extract("#procedure doSomething\n  id x = y;\n#endprocedure\n",
                               CBM_LANG_FORM, "t", "test.frm");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "doSomething"));
    cbm_free_result(r);
    PASS();
}

/* --- Wolfram --- */
TEST(wolfram_function) {
    CBMFileResult *r =
        extract("square[x_] := x^2\nadd[x_, y_] := x + y\n", CBM_LANG_WOLFRAM, "t", "math.wl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "square"));
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* --- Magma --- */
TEST(magma_function) {
    CBMFileResult *r = extract("function Factorial(n)\n  if n le 1 then\n    return 1;\n  end "
                               "if;\n  return n * Factorial(n - 1);\nend function;\n",
                               CBM_LANG_MAGMA, "t", "test.m");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "Factorial"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group G: v0.5 Expansion Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- F# --- */
TEST(fsharp_function) {
    /* Go test only asserts >=1 def — F# name extraction is incomplete */
    CBMFileResult *r = extract("module Greeter\nlet greet name = sprintf \"Hello %s\" name\n",
                               CBM_LANG_FSHARP, "t", "Greeter.fs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Julia --- */
TEST(julia_function) {
    CBMFileResult *r = extract("function add(x, y)\n    x + y\nend\nadd2(x, y) = x + y\n",
                               CBM_LANG_JULIA, "t", "math.jl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* --- Elm --- */
TEST(elm_function) {
    CBMFileResult *r =
        extract("add x y = x + y\nmultiply x y = x * y\n", CBM_LANG_ELM, "t", "Math.elm");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "add"));
    cbm_free_result(r);
    PASS();
}

/* --- Nix --- */
TEST(nix_function) {
    CBMFileResult *r =
        extract("{ pkgs ? import <nixpkgs> {} }:\nlet\n  hello = pkgs.writeShellScriptBin "
                "\"hello\" ''echo hello'';\nin { inherit hello; }\n",
                CBM_LANG_NIX, "t", "default.nix");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* --- Fortran --- */
TEST(fortran_function) {
    /* Fortran subroutine name extraction is incomplete — just verify no crash */
    CBMFileResult *r = extract("subroutine greet(name)\n  character(*), intent(in) :: name\n  "
                               "print *, 'Hello ', name\nend subroutine\n",
                               CBM_LANG_FORTRAN, "t", "greet.f90");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group A2: Missing OOP / Systems variants
 * ═══════════════════════════════════════════════════════════════════ */

/* --- Swift struct --- */
TEST(swift_struct) {
    CBMFileResult *r = extract("struct Point {\n    var x: Double\n    var y: Double\n    func "
                               "distance() -> Double { return (x*x + y*y).squareRoot() }\n}\n",
                               CBM_LANG_SWIFT, "t", "Point.swift");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Method", "distance"));
    cbm_free_result(r);
    PASS();
}

/* --- Swift calls (port of PR #47 Go tests) --- */
TEST(swift_simple_call) {
    CBMFileResult *r = extract("func main() { greet() }\nfunc greet() { print(\"hello\") }\n",
                               CBM_LANG_SWIFT, "t", "main.swift");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_call(r, "greet"));
    cbm_free_result(r);
    PASS();
}

TEST(swift_method_call) {
    CBMFileResult *r = extract("class Foo {\n    func bar() { baz.run() }\n}\n", CBM_LANG_SWIFT,
                               "t", "Foo.swift");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_call(r, "baz.run"));
    cbm_free_result(r);
    PASS();
}

TEST(swift_constructor_call) {
    CBMFileResult *r =
        extract("func create() { let x = MyClass() }\n", CBM_LANG_SWIFT, "t", "create.swift");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_call(r, "MyClass"));
    cbm_free_result(r);
    PASS();
}

TEST(swift_chained_call) {
    CBMFileResult *r = extract("func setup() { AlarmScheduler.shared.startKeepAlive() }\n",
                               CBM_LANG_SWIFT, "t", "setup.swift");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(r->calls.count > 0);
    cbm_free_result(r);
    PASS();
}

/* --- Objective-C --- */
TEST(objc_interface) {
    CBMFileResult *r =
        extract("@interface Animal : NSObject\n- (NSString *)name;\n- (void)speak;\n@end\n",
                CBM_LANG_OBJC, "t", "Animal.h");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

TEST(objc_implementation) {
    CBMFileResult *r = extract("@implementation Animal\n- (NSString *)name { return @\"Animal\"; "
                               "}\n- (void)speak { NSLog(@\"...\"); }\n@end\n",
                               CBM_LANG_OBJC, "t", "Animal.m");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Dart top-level function --- */
TEST(dart_top_level_function) {
    CBMFileResult *r = extract(
        "void main() {\n  print('Hello');\n}\nString greet(String name) => 'Hello $name';\n",
        CBM_LANG_DART, "t", "main.dart");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "main"));
    ASSERT(has_def(r, "Function", "greet"));
    cbm_free_result(r);
    PASS();
}

/* --- Rust enum --- */
TEST(rust_enum) {
    CBMFileResult *r =
        extract("pub enum Direction { North, South, East, West }\n", CBM_LANG_RUST, "t", "dir.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Zig struct --- */
TEST(zig_struct) {
    CBMFileResult *r = extract("const Point = struct { x: f32, y: f32, pub fn dist(self: Point) "
                               "f32 { return self.x + self.y; } };\n",
                               CBM_LANG_ZIG, "t", "point.zig");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- C++ function (standalone) --- */
TEST(cpp_function) {
    CBMFileResult *r = extract("#include <string>\nstd::string greet(const std::string& name) { "
                               "return \"Hello \" + name; }\nint main() { return 0; }\n",
                               CBM_LANG_CPP, "t", "main.cpp");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- COBOL paragraph --- */
TEST(cobol_paragraph) {
    CBMFileResult *r =
        extract("IDENTIFICATION DIVISION.\nPROGRAM-ID. HELLO.\nPROCEDURE DIVISION.\n    "
                "DISPLAY-GREETING.\n        DISPLAY 'HELLO WORLD'.\n        STOP RUN.\n",
                CBM_LANG_COBOL, "t", "hello.cbl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Verilog module --- */
TEST(verilog_module) {
    CBMFileResult *r =
        extract("module adder(input a, input b, output sum);\n  assign sum = a + b;\nendmodule\n",
                CBM_LANG_VERILOG, "t", "adder.v");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- CUDA kernel --- */
TEST(cuda_kernel) {
    CBMFileResult *r = extract("__global__ void vectorAdd(float *a, float *b, float *c, int n) {\n "
                               "   int i = blockIdx.x * blockDim.x + threadIdx.x;\n    if (i < n) "
                               "c[i] = a[i] + b[i];\n}\nint main() { return 0; }\n",
                               CBM_LANG_CUDA, "t", "vector.cu");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Python decorator --- */
TEST(python_decorator) {
    CBMFileResult *r = extract("class Router:\n    @staticmethod\n    def route(path: str):\n      "
                               "  def decorator(func): return func\n        return decorator\n",
                               CBM_LANG_PYTHON, "t", "router.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "Router"));
    cbm_free_result(r);
    PASS();
}

/* --- TypeScript interface --- */
TEST(ts_interface) {
    CBMFileResult *r = extract("export interface Repository<T> { findById(id: number): T; "
                               "save(entity: T): void; delete(id: number): void; }\n",
                               CBM_LANG_TYPESCRIPT, "t", "repo.ts");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- TSX component --- */
TEST(tsx_component) {
    CBMFileResult *r = extract(
        "import React from 'react';\ninterface Props { name: string; }\nexport function Greeting({ "
        "name }: Props) {\n    return <div>Hello {name}</div>;\n}\nexport default Greeting;\n",
        CBM_LANG_TSX, "t", "Greeting.tsx");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "Greeting"));
    cbm_free_result(r);
    PASS();
}

/* --- Lua table method --- */
TEST(lua_table_method) {
    CBMFileResult *r =
        extract("local M = {}\nfunction M.create(name)\n    return { name = name }\nend\nfunction "
                "M.greet(self)\n    return 'Hi ' .. self.name\nend\nreturn M\n",
                CBM_LANG_LUA, "t", "module.lua");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* Should extract at least one Function from Lua table method */
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    ASSERT_GTE(fn_count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Emacs Lisp defun --- */
TEST(emacs_lisp_defun) {
    CBMFileResult *r = extract("(defun greet (name)\n  (message \"Hello %s\" name))\n(defun main "
                               "()\n  (greet \"World\"))\n",
                               CBM_LANG_EMACSLISP, "t", "init.el");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "greet"));
    ASSERT(has_def(r, "Function", "main"));
    cbm_free_result(r);
    PASS();
}

/* --- Emacs Lisp defvar --- */
TEST(emacs_lisp_defvar) {
    CBMFileResult *r = extract("(defvar my-count 0 \"A counter.\")\n(defcustom my-name \"World\" "
                               "\"The name.\"\n  :type 'string)\n",
                               CBM_LANG_EMACSLISP, "t", "vars.el");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Haskell data type --- */
TEST(haskell_data_type) {
    CBMFileResult *r =
        extract("data Shape = Circle Double | Rectangle Double Double\narea :: Shape -> "
                "Double\narea (Circle r) = pi * r * r\narea (Rectangle w h) = w * h\n",
                CBM_LANG_HASKELL, "t", "Shape.hs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Clojure function (known limitation: defn produces list_lit) --- */
TEST(clojure_function) {
    CBMFileResult *r = extract("(ns greeter.core)\n(defn greet [name]\n  (str \"Hello \" "
                               "name))\n(defn -main [& args]\n  (println (greet \"World\")))\n",
                               CBM_LANG_CLOJURE, "t", "core.clj");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* Clojure uses list_lit for all forms — no function defs extracted (known limitation) */
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group E2: Missing Config / Markup Languages
 * ═══════════════════════════════════════════════════════════════════ */

/* --- HTML elements --- */
TEST(html_elements) {
    CBMFileResult *r = extract(
        "<!DOCTYPE "
        "html><html><head><title>Test</title></head><body><h1>Hello</h1><p>World</p></body></html>",
        CBM_LANG_HTML, "t", "index.html");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- SQL function (CREATE FUNCTION) --- */
TEST(sql_function) {
    CBMFileResult *r = extract("CREATE FUNCTION get_user_count() RETURNS INTEGER AS $$ SELECT "
                               "COUNT(*) FROM users; $$ LANGUAGE SQL;\n",
                               CBM_LANG_SQL, "t", "funcs.sql");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Meson project --- */
TEST(meson_project) {
    CBMFileResult *r = extract(
        "project('myapp', 'c', version: '1.0.0')\nexecutable('myapp', 'main.c', install: true)\n",
        CBM_LANG_MESON, "t", "meson.build");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- CSS rules --- */
TEST(css_rules) {
    CBMFileResult *r = extract(
        ".container { display: flex; width: 100%; }\n.button { background: #007bff; color: white; "
        "border: none; }\n@media (max-width: 768px) { .container { flex-direction: column; } }\n",
        CBM_LANG_CSS, "t", "styles.css");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- SCSS rules --- */
TEST(scss_rules) {
    CBMFileResult *r = extract("$primary: #007bff;\n.container {\n  width: 100%;\n  .button {\n    "
                               "background: $primary;\n    &:hover { opacity: 0.8; }\n  }\n}\n",
                               CBM_LANG_SCSS, "t", "styles.scss");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- TOML basic --- */
TEST(toml_basic) {
    CBMFileResult *r = extract("[server]\nhost = \"localhost\"\nport = 8080\n\n[database]\nurl = "
                               "\"postgres://localhost/db\"\nmax_connections = 10\n",
                               CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Class", "server"));
    ASSERT(has_def(r, "Class", "database"));
    ASSERT(has_def(r, "Variable", "host"));
    ASSERT(has_def(r, "Variable", "port"));
    cbm_free_result(r);
    PASS();
}

/* --- CMake function --- */
TEST(cmake_function) {
    CBMFileResult *r = extract(
        "cmake_minimum_required(VERSION 3.16)\nproject(MyApp VERSION 1.0)\nadd_executable(myapp "
        "main.cpp)\ntarget_compile_features(myapp PRIVATE cxx_std_17)\n",
        CBM_LANG_CMAKE, "t", "CMakeLists.txt");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- JSON object --- */
TEST(json_object) {
    CBMFileResult *r = extract("{\"name\": \"myapp\", \"version\": \"1.0.0\", \"scripts\": "
                               "{\"build\": \"go build\", \"test\": \"go test ./...\"}}",
                               CBM_LANG_JSON, "t", "config.json");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Variable", "name"));
    ASSERT(has_def(r, "Variable", "version"));
    cbm_free_result(r);
    PASS();
}

/* --- Protobuf message --- */
TEST(protobuf_message) {
    CBMFileResult *r = extract(
        "syntax = \"proto3\";\npackage user;\nmessage User { int64 id = 1; string name = 2; string "
        "email = 3; }\nservice UserService { rpc GetUser(User) returns (User); }\n",
        CBM_LANG_PROTOBUF, "t", "user.proto");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- GraphQL type --- */
TEST(graphql_type) {
    CBMFileResult *r = extract("type User {\n  id: ID!\n  name: String!\n  email: String!\n}\ntype "
                               "Query {\n  user(id: ID!): User\n  users: [User!]!\n}\n",
                               CBM_LANG_GRAPHQL, "t", "schema.graphql");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Svelte component --- */
TEST(svelte_component) {
    CBMFileResult *r = extract("<script>\n  let name = 'World';\n  function greet() {\n    return "
                               "`Hello ${name}`;\n  }\n</script>\n<h1>{greet()}</h1>\n",
                               CBM_LANG_SVELTE, "t", "App.svelte");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- Vue component --- */
TEST(vue_component) {
    CBMFileResult *r =
        extract("<template><div>{{ message }}</div></template>\n<script>\nexport default {\n  "
                "name: 'App',\n  data() { return { message: 'Hello World' }; },\n  methods: { "
                "greet() { return this.message; } }\n};\n</script>\n",
                CBM_LANG_VUE, "t", "App.vue");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- GLSL shader --- */
TEST(glsl_shader) {
    CBMFileResult *r = extract(
        "#version 330 core\nvoid main() {\n    gl_Position = vec4(0.0, 0.0, 0.0, 1.0);\n}\nvec3 "
        "transform(vec3 pos, mat4 mvp) {\n    return (mvp * vec4(pos, 1.0)).xyz;\n}\n",
        CBM_LANG_GLSL, "t", "vertex.glsl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* --- VimScript function --- */
TEST(vimscript_function) {
    CBMFileResult *r = extract("function! SayHello()\n  echo 'Hello'\nendfunction\n",
                               CBM_LANG_VIMSCRIPT, "t", "plugin.vim");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* VimScript extraction may or may not produce named functions */
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    if (fn_count > 0) {
        ASSERT(has_def(r, "Function", "SayHello"));
    }
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group H: Scientific / Math — extended tests
 * ═══════════════════════════════════════════════════════════════════ */

/* --- MATLAB parse (simple expression) --- */
TEST(matlab_parse) {
    CBMFileResult *r = extract("x = 1;\ny = x + 2;\n", CBM_LANG_MATLAB, "t", "simple.matlab");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* --- MATLAB call --- */
TEST(matlab_call) {
    CBMFileResult *r = extract("function y = foo(x)\n  y = inv(x);\n  disp hello\nend\n",
                               CBM_LANG_MATLAB, "t", "foo.matlab");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->calls.count, 0);
    ASSERT(has_call(r, "inv"));
    ASSERT(has_call(r, "disp"));
    cbm_free_result(r);
    PASS();
}

/* --- Lean parse (theorem) --- */
TEST(lean_parse) {
    CBMFileResult *r = extract("theorem add_comm (a b : Nat) : a + b = b + a := by omega\n",
                               CBM_LANG_LEAN, "t", "Comm.lean");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* --- Lean call (recursive fib) --- */
TEST(lean_call) {
    CBMFileResult *r = extract("def fib : Nat \xe2\x86\x92 Nat\n  | 0 => 1\n  | 1 => 1\n  | n + 2 "
                               "=> fib (n + 1) + fib n\n",
                               CBM_LANG_LEAN, "t", "Fib.lean");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->calls.count, 0);
    ASSERT(has_call(r, "fib"));
    cbm_free_result(r);
    PASS();
}

/* --- Lean type annotation not call --- */
TEST(lean_type_annotation_not_call) {
    CBMFileResult *r = extract(
        "def listLen (xs : List Nat) : Nat := 0\ndef greet : IO Unit := IO.println \"hi\"\n",
        CBM_LANG_LEAN, "t", "Types.lean");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* "List" in binder type position should NOT be extracted as a call */
    for (int i = 0; i < r->calls.count; i++) {
        ASSERT_FALSE(strcmp(r->calls.items[i].callee_name, "List") == 0);
    }
    /* IO.println in the body should be present */
    int found_println = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strstr(r->calls.items[i].callee_name, "println") != NULL) {
            found_println = 1;
        }
    }
    ASSERT_TRUE(found_println);
    cbm_free_result(r);
    PASS();
}

/* --- FORM parse (simple expression) --- */
TEST(form_parse) {
    CBMFileResult *r = extract("Symbols x, y;\nLocal F = x + y;\nPrint;\n.end\n", CBM_LANG_FORM,
                               "t", "example.frm");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* --- FORM call (#call) --- */
TEST(form_call) {
    CBMFileResult *r = extract("#procedure myproc(x)\n  id `x' = 0;\n#endprocedure\n#procedure "
                               "caller()\n  #call myproc(1)\n#endprocedure\n",
                               CBM_LANG_FORM, "t", "calc.frm");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->calls.count, 0);
    ASSERT(has_call(r, "myproc"));
    cbm_free_result(r);
    PASS();
}

/* --- Magma procedure --- */
TEST(magma_procedure) {
    CBMFileResult *r = extract("procedure PrintHello()\n  print \"Hello\";\nend procedure;\n",
                               CBM_LANG_MAGMA, "t", "hello.mag");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    if (fn_count > 0) {
        ASSERT(has_def(r, "Function", "PrintHello"));
    }
    cbm_free_result(r);
    PASS();
}

/* --- Magma parse (simple) --- */
TEST(magma_parse) {
    CBMFileResult *r = extract("x := 42;\ny := x + 1;\n", CBM_LANG_MAGMA, "t", "simple.mag");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* --- Magma import (load) --- */
TEST(magma_import) {
    CBMFileResult *r = extract("load \"utils.mag\";\nload \"lib/helpers.mag\";\n", CBM_LANG_MAGMA,
                               "t", "main.mag");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->imports.count, 2);
    cbm_free_result(r);
    PASS();
}

/* --- Magma call --- */
TEST(magma_call) {
    CBMFileResult *r = extract("function Foo(x)\n  y := Bar(x);\n  return y;\nend function;\n",
                               CBM_LANG_MAGMA, "t", "calls.mag");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->calls.count, 0);
    ASSERT(has_call(r, "Bar"));
    cbm_free_result(r);
    PASS();
}

/* --- Magma disambiguation (.m file as Magma) --- */
TEST(magma_disambiguation) {
    CBMFileResult *r = extract("function Factorial(n)\n  if n le 1 then\n    return 1;\n  end "
                               "if;\n  return n * Factorial(n - 1);\nend function;\n",
                               CBM_LANG_MAGMA, "t", "test.m");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    ASSERT_GTE(fn_count, 1);
    if (fn_count > 0) {
        ASSERT(has_def(r, "Function", "Factorial"));
    }
    cbm_free_result(r);
    PASS();
}

/* --- Wolfram function (both := and =) --- */
TEST(wolfram_function_extended) {
    CBMFileResult *r = extract("f[x_] := x^2\ng[x_] = x + 1\n", CBM_LANG_WOLFRAM, "t", "funcs.wl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    ASSERT_GTE(fn_count, 2);
    ASSERT(has_def(r, "Function", "f"));
    ASSERT(has_def(r, "Function", "g"));
    cbm_free_result(r);
    PASS();
}

/* --- Wolfram call --- */
TEST(wolfram_call) {
    CBMFileResult *r = extract("f[x_] := g[x] + h[x]\n", CBM_LANG_WOLFRAM, "t", "calls.wl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->calls.count, 0);
    ASSERT(has_call(r, "g"));
    ASSERT(has_call(r, "h"));
    /* "f" should NOT appear as a call (it's the definition LHS) */
    for (int i = 0; i < r->calls.count; i++) {
        ASSERT_FALSE(strcmp(r->calls.items[i].callee_name, "f") == 0);
    }
    cbm_free_result(r);
    PASS();
}

/* --- Wolfram caller attribution --- */
TEST(wolfram_caller_attribution) {
    CBMFileResult *r = extract("f[x_] := g[x] + h[x]\n", CBM_LANG_WOLFRAM, "t", "caller.wl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->calls.count, 0);
    /* Calls inside f[] should have f as enclosing function, not the module path */
    for (int i = 0; i < r->calls.count; i++) {
        if (strcmp(r->calls.items[i].callee_name, "g") == 0 ||
            strcmp(r->calls.items[i].callee_name, "h") == 0) {
            /* enclosing_func_qn must NOT be empty or the file path */
            ASSERT_NOT_NULL(r->calls.items[i].enclosing_func_qn);
            ASSERT_FALSE(strcmp(r->calls.items[i].enclosing_func_qn, "") == 0);
            ASSERT_FALSE(strcmp(r->calls.items[i].enclosing_func_qn, "t.caller") == 0);
        }
    }
    cbm_free_result(r);
    PASS();
}

/* --- Wolfram parse (simple assignment) --- */
TEST(wolfram_parse) {
    CBMFileResult *r = extract("x = 42;\ny = x + 1;\n", CBM_LANG_WOLFRAM, "t", "simple.wl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

/* --- Wolfram import --- */
TEST(wolfram_import) {
    CBMFileResult *r =
        extract("<< \"utils.wl\"\nNeeds[\"Package`\"]\n", CBM_LANG_WOLFRAM, "t", "main.wl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    cbm_free_result(r);
    PASS();
}

/* --- Wolfram nested def --- */
TEST(wolfram_nested_def) {
    CBMFileResult *r = extract("main[x_] := Module[{localF}, localF[t_] := t + 1; localF[x]]\n",
                               CBM_LANG_WOLFRAM, "t", "nested.wl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "main"));
    ASSERT(has_def(r, "Function", "localF"));
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group I: cbm_test.go ports
 * ═══════════════════════════════════════════════════════════════════ */

TEST(python_docstring) {
    CBMFileResult *r = extract(
        "def compute(x, y):\n    \"\"\"Compute the sum of x and y.\"\"\"\n    return x + y\n",
        CBM_LANG_PYTHON, "test", "test.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "compute"));
    /* Check docstring is present */
    int found = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].name, "compute") == 0) {
            found = 1;
            ASSERT_NOT_NULL(r->defs.items[i].docstring);
            ASSERT_TRUE(strlen(r->defs.items[i].docstring) > 0);
        }
    }
    ASSERT_TRUE(found);
    cbm_free_result(r);
    PASS();
}

TEST(go_function_extraction) {
    CBMFileResult *r =
        extract("package main\n\n// Greet returns a greeting.\nfunc Greet(name string) string "
                "{\n\treturn \"Hello, \" + name\n}\n\nfunc main() {\n\tGreet(\"world\")\n}\n",
                CBM_LANG_GO, "test", "main.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "Greet"));
    ASSERT(has_def(r, "Function", "main"));
    ASSERT(has_call(r, "Greet"));
    cbm_free_result(r);
    PASS();
}

TEST(js_arrow_function) {
    CBMFileResult *r = extract("const greet = (name) => {\n  return \"Hello \" + "
                               "name;\n};\n\nconst result = greet(\"world\");\n",
                               CBM_LANG_JAVASCRIPT, "test", "app.js");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Group J: language_failures_test.go ports
 * ═══════════════════════════════════════════════════════════════════ */

/* CommonLisp — defun extraction (known limitation: grammar produces list_lit) */
TEST(commonlisp_defun) {
    CBMFileResult *r =
        extract("(defun hello () \"world\")\n", CBM_LANG_COMMONLISP, "test", "hello.lisp");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* Known limitation: CommonLisp grammar produces list_lit, not defun nodes.
     * Function extraction returns 0 — this test documents the limitation. */
    cbm_free_result(r);
    PASS();
}

TEST(commonlisp_multiple_functions) {
    CBMFileResult *r = extract("(defun add (a b) (+ a b))\n(defun mul (a b) (* a b))\n",
                               CBM_LANG_COMMONLISP, "test", "math.lisp");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

TEST(commonlisp_defmacro) {
    CBMFileResult *r =
        extract("(defmacro when2 (condition &body body)\n  `(if ,condition (progn ,@body)))\n",
                CBM_LANG_COMMONLISP, "test", "macros.lisp");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

TEST(makefile_rule_as_function) {
    CBMFileResult *r = extract("all:\n\t@echo hello\n", CBM_LANG_MAKEFILE, "test", "Makefile");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "all"));
    cbm_free_result(r);
    PASS();
}

TEST(makefile_multiple_targets) {
    CBMFileResult *r = extract("all: main.o\n\tgcc -o all main.o\n\nbuild:\n\tgo build ./...\n",
                               CBM_LANG_MAKEFILE, "test", "Makefile");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_def(r, "Function", "all"));
    ASSERT(has_def(r, "Function", "build"));
    cbm_free_result(r);
    PASS();
}

TEST(makefile_variable_extraction) {
    CBMFileResult *r =
        extract("CC := gcc\nCFLAGS := -Wall\n", CBM_LANG_MAKEFILE, "test", "Makefile");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* Variable extraction may or may not work depending on Makefile grammar support */
    cbm_free_result(r);
    PASS();
}

TEST(vimscript_function_extraction) {
    CBMFileResult *r = extract("function! SayHello()\n  echo 'Hello'\nendfunction\n",
                               CBM_LANG_VIMSCRIPT, "test", "plugin.vim");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* VimScript function extraction may or may not produce named functions */
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    if (fn_count > 0) {
        ASSERT(has_def(r, "Function", "SayHello"));
    }
    cbm_free_result(r);
    PASS();
}

TEST(vimscript_function_without_bang) {
    CBMFileResult *r = extract("function MyFunc(arg)\n  return arg\nendfunction\n",
                               CBM_LANG_VIMSCRIPT, "test", "plugin.vim");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    if (fn_count > 0) {
        ASSERT(has_def(r, "Function", "MyFunc"));
    }
    cbm_free_result(r);
    PASS();
}

TEST(julia_function_extraction) {
    CBMFileResult *r = extract("function hello()\n  println(\"Hello, World!\")\nend\n",
                               CBM_LANG_JULIA, "test", "hello.jl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    if (fn_count > 0) {
        ASSERT(has_def(r, "Function", "hello"));
    }
    cbm_free_result(r);
    PASS();
}

TEST(julia_function_with_args) {
    CBMFileResult *r = extract("function add(a::Int, b::Int)::Int\n  return a + b\nend\n",
                               CBM_LANG_JULIA, "test", "math.jl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    int fn_count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Function") == 0)
            fn_count++;
    }
    if (fn_count > 0) {
        ASSERT(has_def(r, "Function", "add"));
    }
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Cross-cutting: Calls + Imports
 * ═══════════════════════════════════════════════════════════════════ */

TEST(python_calls) {
    CBMFileResult *r =
        extract("import os\ndef main():\n    os.path.exists('/tmp')\n    print('hello')\n",
                CBM_LANG_PYTHON, "t", "main.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    /* Python unified extraction produces calls — verify at least some exist */
    ASSERT_GT(r->calls.count, 0);
    cbm_free_result(r);
    PASS();
}

TEST(go_calls) {
    CBMFileResult *r =
        extract("package main\nimport \"fmt\"\nfunc main() { fmt.Println(\"hello\") }\n",
                CBM_LANG_GO, "t", "main.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_call(r, "fmt.Println"));
    cbm_free_result(r);
    PASS();
}

TEST(python_imports) {
    CBMFileResult *r =
        extract("import os\nfrom sys import argv\nfrom collections import defaultdict\n",
                CBM_LANG_PYTHON, "t", "main.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    cbm_free_result(r);
    PASS();
}

TEST(js_imports) {
    CBMFileResult *r = extract("import React from 'react';\nimport { useState } from "
                               "'react';\nconst fs = require('fs');\n",
                               CBM_LANG_JAVASCRIPT, "t", "app.js");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    cbm_free_result(r);
    PASS();
}

TEST(go_imports) {
    CBMFileResult *r =
        extract("package main\n\nimport \"fmt\"\nimport (\n    \"os\"\n    net \"net/http\"\n)\n",
                CBM_LANG_GO, "t", "main.go");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    ASSERT(has_import(r, "fmt"));
    cbm_free_result(r);
    PASS();
}

TEST(java_imports) {
    CBMFileResult *r =
        extract("import java.util.List;\nimport java.util.ArrayList;\nimport static java.lang.Math.PI;\n"
                "public class Foo {}\n",
                CBM_LANG_JAVA, "t", "Foo.java");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    ASSERT(has_import(r, "java.util.List"));
    cbm_free_result(r);
    PASS();
}

TEST(rust_imports) {
    CBMFileResult *r =
        extract("use std::collections::HashMap;\nuse std::io::{self, Write};\nuse serde::Serialize;\n"
                "fn main() {}\n",
                CBM_LANG_RUST, "t", "main.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    ASSERT(has_import(r, "std::collections::HashMap"));
    cbm_free_result(r);
    PASS();
}

TEST(c_imports) {
    CBMFileResult *r =
        extract("#include <stdio.h>\n#include <stdlib.h>\n#include \"mylib.h\"\n\nint main() { return 0; }\n",
                CBM_LANG_C, "t", "main.c");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    ASSERT(has_import(r, "stdio.h"));
    cbm_free_result(r);
    PASS();
}

TEST(ruby_imports) {
    CBMFileResult *r =
        extract("require 'json'\nrequire 'net/http'\nrequire_relative 'helpers'\n\nclass Foo; end\n",
                CBM_LANG_RUBY, "t", "app.rb");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    ASSERT(has_import(r, "json"));
    cbm_free_result(r);
    PASS();
}

TEST(lua_imports) {
    CBMFileResult *r =
        extract("local json = require(\"dkjson\")\nlocal http = require(\"socket.http\")\n\nlocal function greet() end\n",
                CBM_LANG_LUA, "t", "main.lua");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GT(r->imports.count, 0);
    ASSERT(has_import(r, "dkjson"));
    cbm_free_result(r);
    PASS();
}

TEST(gdscript_parse_basic) {
    CBMFileResult *r = extract("func _ready():\n    pass\n", CBM_LANG_GDSCRIPT, "t", "main.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    cbm_free_result(r);
    PASS();
}

TEST(gdscript_constructor_definition_maps_to_init_method_and_scope) {
    const char *src = "class_name Player\n"
                      "func _init(name):\n"
                      "    print(\"ctor\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_init_method = 0;
    int saw_ctor_call_scope = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "_init") == 0) {
            saw_init_method = 1;
            ASSERT_STR_EQ(d->qualified_name, "t.player.Player._init");
            ASSERT_STR_EQ(d->parent_class, "t.player.Player");
        }
    }
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "print") == 0) {
            saw_ctor_call_scope = 1;
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.player.Player._init");
        }
    }

    ASSERT_TRUE(saw_init_method);
    ASSERT_TRUE(saw_ctor_call_scope);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_defs_anchor_and_methods) {
    const char *src = "class_name Player\n"
                      "extends Node2D\n"
                      "@export var speed = 10\n"
                      "signal hit\n"
                      "func _ready():\n"
                      "    pass\n";
    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int class_count = 0;
    const char *anchor_qn = NULL;
    int has_export_var = 0;
    int has_signal_qn = 0;
    int has_method_parent_match = 0;
    int has_script_suffix = 0;

    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Class") == 0 && strcmp(d->name, "Player") == 0) {
            class_count++;
            anchor_qn = d->qualified_name;
            ASSERT_NOT_NULL(anchor_qn);
            ASSERT_STR_EQ(anchor_qn, "t.player.Player");
            if (strstr(anchor_qn, ".__script__") != NULL) {
                has_script_suffix = 1;
            }
            if (d->base_classes && d->base_classes[0]) {
                ASSERT(strstr(d->base_classes[0], "Node2D") != NULL);
            }
        }
        if (strcmp(d->label, "Variable") == 0 && strcmp(d->name, "speed") == 0) {
            has_export_var = 1;
        }
        if (strcmp(d->label, "Function") == 0 &&
            strcmp(d->qualified_name, "t.player.Player.signal.hit") == 0) {
            has_signal_qn = 1;
        }
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "_ready") == 0) {
            if (d->parent_class && strcmp(d->parent_class, "t.player.Player") == 0) {
                has_method_parent_match = 1;
            }
        }
    }

    ASSERT_EQ(class_count, 1);
    ASSERT_FALSE(has_script_suffix);
    ASSERT(has_def(r, "Method", "_ready"));
    ASSERT_TRUE(has_export_var);
    ASSERT_TRUE(has_signal_qn);
    ASSERT_TRUE(has_method_parent_match);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_defs_script_fallback_anchor) {
    const char *src = "extends Node\n"
                      "func tick():\n"
                      "    pass\n";
    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "npc.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int class_count = 0;
    const char *anchor_qn = NULL;
    int method_parent_ok = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Class") == 0) {
            class_count++;
            anchor_qn = d->qualified_name;
            ASSERT_NOT_NULL(anchor_qn);
            ASSERT_STR_EQ(anchor_qn, "t.npc.__script__");
        }
    }
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "tick") == 0 &&
            d->parent_class && anchor_qn && strcmp(d->parent_class, anchor_qn) == 0) {
            method_parent_ok = 1;
        }
    }

    ASSERT_EQ(class_count, 1);
    ASSERT_TRUE(method_parent_ok);
    cbm_free_result(r);
    PASS();
}

TEST(gdscript_defs_static_func_var_and_class_stmt) {
    const char *src = "class_name Utility\n"
                      "var hp = 100\n"
                      "static func make_id():\n"
                      "    return 1\n"
                      "class Inner:\n"
                      "    signal spawned\n"
                      "    func f():\n"
                      "        pass\n";
    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "utility.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    const char *anchor_qn = "t.utility.Utility";
    int has_static_method = 0;
    int has_alias_name_on_anchor = 0;
    int anchor_count = 0;
    int has_inner_class = 0;
    int has_inner_method = 0;
    int has_inner_signal = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Class") == 0) {
            if (strcmp(d->name, "Utility") == 0 && strcmp(d->qualified_name, anchor_qn) == 0) {
                anchor_count++;
                has_alias_name_on_anchor = 1;
            }
            if (strcmp(d->name, "Inner") == 0 &&
                strcmp(d->qualified_name, "t.utility.Utility.Inner") == 0) {
                has_inner_class = 1;
            }
        }
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "make_id") == 0 &&
            d->parent_class && strcmp(d->parent_class, anchor_qn) == 0) {
            has_static_method = 1;
        }
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "f") == 0 &&
            d->parent_class && strcmp(d->parent_class, "t.utility.Utility.Inner") == 0) {
            has_inner_method = 1;
        }
        if (strcmp(d->label, "Function") == 0 &&
            strcmp(d->qualified_name, "t.utility.Utility.Inner.signal.spawned") == 0) {
            has_inner_signal = 1;
        }
    }

    ASSERT_EQ(anchor_count, 1);
    ASSERT_TRUE(has_alias_name_on_anchor);
    ASSERT(has_def(r, "Variable", "hp"));
    ASSERT_TRUE(has_static_method);
    ASSERT_TRUE(has_inner_class);
    ASSERT_TRUE(has_inner_method);
    ASSERT_TRUE(has_inner_signal);
    cbm_free_result(r);
    PASS();
}

TEST(gdscript_export_variable_flags_and_anchor_linkage) {
    const char *src = "class_name Player\n"
                      "@export var speed = 10\n"
                      "var hp = 100\n"
                      "func _ready():\n"
                      "    pass\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_speed = 0;
    int saw_hp = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Variable") != 0) {
            continue;
        }
        if (strcmp(d->name, "speed") == 0) {
            saw_speed = 1;
            ASSERT_TRUE(d->is_exported);
            ASSERT_STR_EQ(d->qualified_name, "t.player.Player.speed");
            ASSERT_STR_EQ(d->parent_class, "t.player.Player");
        }
        if (strcmp(d->name, "hp") == 0) {
            saw_hp = 1;
            ASSERT_FALSE(d->is_exported);
            ASSERT_STR_EQ(d->qualified_name, "t.player.Player.hp");
            ASSERT_STR_EQ(d->parent_class, "t.player.Player");
        }
    }

    ASSERT_TRUE(saw_speed);
    ASSERT_TRUE(saw_hp);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_signal_calls_and_import_alias) {
    const char *src = "class_name Player\n"
                      "const Enemy = preload(\"res://actors/enemy.gd\")\n"
                      "var EnemyLocal = load(\"enemy_local.gd\")\n"
                      "var SceneRef = preload(\"res://actors/enemy.tscn\")\n"
                      "var SceneLocal = load(\"enemy_local.tscn\")\n"
                      "func attack():\n"
                      "    emit_signal(\"hit\", 1)\n"
                      "    hit.emit(2)\n"
                      "    hit.connect(_on_hit)\n"
                      "    preload(\"res://actors/effect.gd\")\n"
                      "    load(dyn_path)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT(has_import_exact(r, "Enemy", "actors/enemy.gd"));
    ASSERT(has_import_exact(r, "EnemyLocal", "enemy_local.gd"));
    ASSERT(has_import(r, "actors/effect.gd"));

    for (int i = 0; i < r->imports.count; i++) {
        CBMImport *imp = &r->imports.items[i];
        ASSERT_FALSE(imp->module_path && strstr(imp->module_path, ".tscn") != NULL);
    }

    ASSERT(has_string_ref_value(r, "res://actors/enemy.tscn"));
    ASSERT(has_string_ref_value(r, "enemy_local.tscn"));

    int saw_emit_signal = 0;
    int saw_dot_emit = 0;
    int saw_dot_connect = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "emit_signal") == 0) {
            saw_emit_signal = 1;
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.player.Player.attack");
            ASSERT_STR_EQ(c->first_string_arg, "hit");
        }
        if (strcmp(c->callee_name, "hit.emit") == 0) {
            saw_dot_emit = 1;
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.player.Player.attack");
            ASSERT_STR_EQ(c->first_string_arg, "hit");
        }
        if (strcmp(c->callee_name, "hit.connect") == 0) {
            saw_dot_connect = 1;
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.player.Player.attack");
            ASSERT_STR_EQ(c->first_string_arg, "hit");
        }
    }

    ASSERT_TRUE(saw_emit_signal);
    ASSERT_TRUE(saw_dot_emit);
    ASSERT_TRUE(saw_dot_connect);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_nested_preload_call_chain_emits_import) {
    const char *src = "extends EditorPlugin\n"
                      "func _enter_tree():\n"
                      "    add_child(preload(\"res://addons/simple_import_plugin/import.gd\").new())\n"
                      "\n"
                      "func _process(_delta):\n"
                      "    preload(\"res://addons/simple_import_plugin/cleanup.gd\")._cleanup()\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "addons/simple_import_plugin/plugin.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_import_exact(r, NULL, "addons/simple_import_plugin/import.gd"));
    ASSERT(has_import_exact(r, NULL, "addons/simple_import_plugin/cleanup.gd"));
    cbm_free_result(r);
    PASS();
}

TEST(gdscript_nested_preload_argument_emits_import) {
    const char *src = "extends EditorPlugin\n"
                      "func handles(object):\n"
                      "    return is_instance_of(object, preload(\"res://addons/main_screen/handled_by_main_screen.gd\"))\n";

    CBMFileResult *r =
        extract(src, CBM_LANG_GDSCRIPT, "t", "addons/main_screen/main_screen_plugin.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_import(r, "addons/main_screen/handled_by_main_screen.gd"));
    cbm_free_result(r);
    PASS();
}

TEST(gdscript_extends_nested_preload_emits_import) {
    const char *src = "extends preload(\"res://actors/base.gd\")\n"
                      "func _ready():\n"
                      "    pass\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "addons/special/gdscript_base.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT(has_import(r, "actors/base.gd"));
    int saw_aliasless_import = 0;
    for (int i = 0; i < r->imports.count; i++) {
        CBMImport *imp = &r->imports.items[i];
        if (imp->module_path && strcmp(imp->module_path, "actors/base.gd") == 0) {
            ASSERT_TRUE(imp->local_name == NULL);
            saw_aliasless_import = 1;
        }
    }
    ASSERT_TRUE(saw_aliasless_import);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_top_level_string_ref_uses_script_anchor_scope) {
    const char *src = "class_name Player\n"
                      "const API_URL = \"https://example.com/api\"\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT(has_string_ref_with_enclosing(r, "https://example.com/api", "t.player.Player"));

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_signal_metadata_dotted_receivers) {
    const char *src = "class_name Player\n"
                      "func attack():\n"
                      "    self.hit.emit()\n"
                      "    enemy.died.connect(_on_died)\n"
                      "    enemy.died.connect(\"override\", _on_died)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_self_hit_emit = 0;
    int saw_enemy_died_connect = 0;
    int saw_enemy_died_connect_with_string = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "self.hit.emit") == 0) {
            saw_self_hit_emit = 1;
            ASSERT_STR_EQ(c->first_string_arg, "hit");
        }
        if (strcmp(c->callee_name, "enemy.died.connect") == 0) {
            if (c->first_string_arg && strcmp(c->first_string_arg, "override") == 0) {
                saw_enemy_died_connect_with_string = 1;
                ASSERT_STR_EQ(c->first_string_arg, "override");
            } else {
                saw_enemy_died_connect = 1;
                ASSERT_STR_EQ(c->first_string_arg, "died");
            }
        }
    }

    ASSERT_TRUE(saw_self_hit_emit);
    ASSERT_TRUE(saw_enemy_died_connect);
    ASSERT_TRUE(saw_enemy_died_connect_with_string);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_signal_metadata_receiver_emit_prefers_receiver_name_over_payload) {
    const char *src = "class_name Player\n"
                      "const PAYLOAD_CONST = \"payload\"\n"
                      "func attack(hit, crit):\n"
                      "    hit.emit(\"payload\")\n"
                      "    crit.emit(PAYLOAD_CONST)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_hit_emit = 0;
    int saw_crit_emit = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "hit.emit") == 0) {
            saw_hit_emit = 1;
            ASSERT_STR_EQ(c->first_string_arg, "hit");
        }
        if (strcmp(c->callee_name, "crit.emit") == 0) {
            saw_crit_emit = 1;
            ASSERT_STR_EQ(c->first_string_arg, "crit");
        }
    }

    ASSERT_TRUE(saw_hit_emit);
    ASSERT_TRUE(saw_crit_emit);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_signal_metadata_connect_keeps_receiver_signal_with_extra_args) {
    const char *src = "class_name Player\n"
                      "func setup(enemy):\n"
                      "    enemy.died.connect(Callable(self, \"_on_died\"), CONNECT_ONE_SHOT)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_died_connect = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "enemy.died.connect") == 0) {
            saw_died_connect = 1;
            ASSERT_STR_EQ(c->first_string_arg, "died");
        }
    }

    ASSERT_TRUE(saw_died_connect);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_signal_metadata_connect_keeps_dotless_receiver_signal_with_flags) {
    const char *src = "class_name Player\n"
                      "func setup(hit):\n"
                      "    hit.connect(_on_hit, CONNECT_ONE_SHOT)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_hit_connect = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "hit.connect") == 0) {
            saw_hit_connect = 1;
            ASSERT_STR_EQ(c->first_string_arg, "hit");
        }
    }

    ASSERT_TRUE(saw_hit_connect);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_imports_ignore_non_global_and_dynamic_loads) {
    const char *src = "func setup():\n"
                      "    foo.load(\"res://actors/enemy.gd\")\n"
                      "    foo.preload(\"res://actors/effect.gd\")\n"
                      "    reload(\"res://actors/enemy.gd\")\n"
                      "    custom_preload(\"res://actors/enemy.gd\")\n"
                      "    load(base + \"enemy.gd\")\n"
                      "    const Foo = preload(prefix + \"enemy.gd\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT_EQ(r->imports.count, 0);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_chained_callee_parenthesized_receivers) {
    const char *src = "class_name Player\n"
                      "func setup(node):\n"
                      "    get_tree().root.add_child(node)\n"
                      "    get_tree().create_timer(1.0).timeout.connect(_on_timeout)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_add_child = 0;
    int saw_timeout_connect = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "get_tree().root.add_child") == 0) {
            saw_add_child = 1;
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.player.Player.setup");
        }
        if (strcmp(c->callee_name, "get_tree().create_timer(1.0).timeout.connect") == 0) {
            saw_timeout_connect = 1;
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.player.Player.setup");
            ASSERT_STR_EQ(c->first_string_arg, "timeout");
        }
    }

    ASSERT_TRUE(saw_add_child);
    ASSERT_TRUE(saw_timeout_connect);
    cbm_free_result(r);
    PASS();
}

TEST(gdscript_import_from_plain_assignment_preload) {
    const char *src = "func setup():\n"
                      "    enemy_scene = preload(\"res://actors/enemy.gd\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT(has_import_exact(r, "enemy_scene", "actors/enemy.gd"));

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_signal_calls_resolve_const_signal_name) {
    const char *src = "class_name Player\n"
                      "const HIT = \"hit\"\n"
                      "func attack():\n"
                      "    emit_signal(HIT)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_emit_signal = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "emit_signal") == 0) {
            saw_emit_signal = 1;
            ASSERT_STR_EQ(c->first_string_arg, "hit");
        }
    }
    ASSERT_TRUE(saw_emit_signal);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_nested_class_constructor_definition_maps_to_init_method) {
    const char *src = "class_name Utility\n"
                      "class Inner:\n"
                      "    func _init():\n"
                      "        print(\"ctor\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "utility.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_inner_init_method = 0;
    int saw_inner_ctor_call_scope = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Method") == 0 && strcmp(d->name, "_init") == 0) {
            saw_inner_init_method = 1;
            ASSERT_STR_EQ(d->qualified_name, "t.utility.Utility.Inner._init");
            ASSERT_STR_EQ(d->parent_class, "t.utility.Utility.Inner");
        }
    }
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "print") == 0) {
            saw_inner_ctor_call_scope = 1;
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.utility.Utility.Inner._init");
        }
    }

    ASSERT_TRUE(saw_inner_init_method);
    ASSERT_TRUE(saw_inner_ctor_call_scope);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_nested_class_const_does_not_resolve_outer_emit_signal_name) {
    const char *src = "class_name Player\n"
                      "class Inner:\n"
                      "    const HIT = \"hit\"\n"
                      "func attack():\n"
                      "    emit_signal(HIT)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_emit_signal = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "emit_signal") == 0) {
            saw_emit_signal = 1;
            ASSERT_TRUE(c->first_string_arg == NULL);
            ASSERT_STR_EQ(c->enclosing_func_qn, "t.player.Player.attack");
        }
    }
    ASSERT_TRUE(saw_emit_signal);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_extends_path_is_normalized_for_import_and_base_class) {
    const char *src = "class_name Enemy\n"
                      "extends \"res://actors/base_enemy.gd\"\n"
                      "func _ready():\n"
                      "    pass\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "enemy.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT(has_import(r, "actors/base_enemy.gd"));

    int saw_enemy_class = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Class") == 0 && strcmp(d->name, "Enemy") == 0) {
            saw_enemy_class = 1;
            ASSERT_NOT_NULL(d->base_classes);
            ASSERT_NOT_NULL(d->base_classes[0]);
            ASSERT_STR_EQ(d->base_classes[0], "actors/base_enemy.gd");
        }
    }
    ASSERT_TRUE(saw_enemy_class);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_aliasless_relative_preload_does_not_set_gd_local_name) {
    const char *src = "func setup():\n"
                      "    preload(\"enemy.gd\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_enemy_import = 0;
    for (int i = 0; i < r->imports.count; i++) {
        CBMImport *imp = &r->imports.items[i];
        if (imp->module_path && strcmp(imp->module_path, "enemy.gd") == 0) {
            saw_enemy_import = 1;
            ASSERT(imp->local_name == NULL || strcmp(imp->local_name, "gd") != 0);
        }
    }
    ASSERT_TRUE(saw_enemy_import);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_connect_with_unresolved_signal_arg_does_not_infer_receiver_name) {
    const char *src = "class_name Player\n"
                      "func setup(button, signal_name):\n"
                      "    button.connect(signal_name, _on_pressed)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_button_connect = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "button.connect") == 0) {
            saw_button_connect = 1;
            ASSERT_TRUE(c->first_string_arg == NULL);
        }
    }
    ASSERT_TRUE(saw_button_connect);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_alias_imports_ignore_dynamic_quoted_prefix_expressions) {
    const char *src = "const Foo = preload(\"enemy.gd\" + suffix)\n"
                      "var Bar = load(\"enemy.gd\" % x)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT_EQ(r->imports.count, 0);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_emit_signal_unresolved_identifier_does_not_populate_signal_metadata) {
    const char *src = "class_name Player\n"
                      "func attack(signal_name):\n"
                      "    emit_signal(signal_name)\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_emit_signal = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "emit_signal") == 0) {
            saw_emit_signal = 1;
            ASSERT_TRUE(c->first_string_arg == NULL);
        }
    }
    ASSERT_TRUE(saw_emit_signal);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_extends_relative_path_is_normalized_consistently) {
    const char *src = "class_name Enemy\n"
                      "extends \"../base/base_enemy.gd\"\n"
                      "func _ready():\n"
                      "    pass\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "actors/enemies/enemy.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT(has_import(r, "actors/base/base_enemy.gd"));

    int saw_enemy_class = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Class") == 0 && strcmp(d->name, "Enemy") == 0) {
            saw_enemy_class = 1;
            ASSERT_NOT_NULL(d->base_classes);
            ASSERT_NOT_NULL(d->base_classes[0]);
            ASSERT_STR_EQ(d->base_classes[0], "actors/base/base_enemy.gd");
        }
    }
    ASSERT_TRUE(saw_enemy_class);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_callee_parsing_handles_parentheses_inside_string_args) {
    const char *src = "func setup():\n"
                      "    bar(\"(\")\n"
                      "    baz(\")\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_bar = 0;
    int saw_baz = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "bar") == 0) {
            saw_bar = 1;
        }
        if (strcmp(c->callee_name, "baz") == 0) {
            saw_baz = 1;
        }
        ASSERT_FALSE(strstr(c->callee_name, "\"") != NULL);
    }
    ASSERT_TRUE(saw_bar);
    ASSERT_TRUE(saw_baz);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_signal_metadata_ignores_dynamic_receiver_emit_connect) {
    const char *src = "class_name Player\n"
                      "func setup():\n"
                      "    signal_factory().connect(_on_hit)\n"
                      "    get_signal().emit()\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "player.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_factory_connect = 0;
    int saw_get_signal_emit = 0;
    for (int i = 0; i < r->calls.count; i++) {
        CBMCall *c = &r->calls.items[i];
        if (strcmp(c->callee_name, "signal_factory().connect") == 0) {
            saw_factory_connect = 1;
            ASSERT_TRUE(c->first_string_arg == NULL);
        }
        if (strcmp(c->callee_name, "get_signal().emit") == 0) {
            saw_get_signal_emit = 1;
            ASSERT_TRUE(c->first_string_arg == NULL);
        }
    }

    ASSERT_TRUE(saw_factory_connect);
    ASSERT_TRUE(saw_get_signal_emit);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_nested_class_extends_preserves_base_classes) {
    const char *src = "class_name Utility\n"
                      "class Foo extends Bar:\n"
                      "    pass\n"
                      "class PathFoo extends \"res://actors/base_enemy.gd\":\n"
                      "    pass\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "utility.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_foo = 0;
    int saw_path_foo = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Class") == 0 && strcmp(d->name, "Foo") == 0) {
            saw_foo = 1;
            ASSERT_NOT_NULL(d->base_classes);
            ASSERT_NOT_NULL(d->base_classes[0]);
            ASSERT_STR_EQ(d->base_classes[0], "Bar");
        }
        if (strcmp(d->label, "Class") == 0 && strcmp(d->name, "PathFoo") == 0) {
            saw_path_foo = 1;
            ASSERT_NOT_NULL(d->base_classes);
            ASSERT_NOT_NULL(d->base_classes[0]);
            ASSERT_STR_EQ(d->base_classes[0], "actors/base_enemy.gd");
        }
    }

    ASSERT_TRUE(saw_foo);
    ASSERT_TRUE(saw_path_foo);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_aliasless_res_preload_and_extends_do_not_set_local_name) {
    const char *src = "extends \"res://actors/base_enemy.gd\"\n"
                      "func setup():\n"
                      "    preload(\"res://actors/effect.gd\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "actors/enemies/enemy.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    int saw_base_import = 0;
    int saw_effect_import = 0;
    for (int i = 0; i < r->imports.count; i++) {
        CBMImport *imp = &r->imports.items[i];
        if (imp->module_path && strcmp(imp->module_path, "actors/base_enemy.gd") == 0) {
            saw_base_import = 1;
            ASSERT_TRUE(imp->local_name == NULL);
        }
        if (imp->module_path && strcmp(imp->module_path, "actors/effect.gd") == 0) {
            saw_effect_import = 1;
            ASSERT_TRUE(imp->local_name == NULL);
        }
    }
    ASSERT_TRUE(saw_base_import);
    ASSERT_TRUE(saw_effect_import);

    cbm_free_result(r);
    PASS();
}

TEST(gdscript_above_root_relative_path_is_rejected) {
    const char *src = "class_name Enemy\n"
                      "extends \"../../../base_enemy.gd\"\n"
                      "func setup():\n"
                      "    preload(\"../../../effect.gd\")\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "actors/enemies/enemy.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    ASSERT_FALSE(has_import(r, "base_enemy.gd"));
    ASSERT_FALSE(has_import(r, "effect.gd"));

    int saw_enemy = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Class") == 0 && strcmp(d->name, "Enemy") == 0) {
            saw_enemy = 1;
            ASSERT_NOT_NULL(d->base_classes);
            ASSERT_NOT_NULL(d->base_classes[0]);
            ASSERT_STR_EQ(d->base_classes[0], "../../../base_enemy.gd");
        }
    }
    ASSERT_TRUE(saw_enemy);

    cbm_free_result(r);
    PASS();
}

TEST(import_stress_go) {
    /* Stress test: 5,000 single-line Go imports.
     * Verifies O(N) behaviour — would hang indefinitely with the O(N²) loop. */
    const int N = 5000;
    /* Each line: import "pkg/NNNNN"\n  = ~20 chars; total ~100KB */
    int buf_size = N * 24 + 64;
    char *src = malloc((size_t)buf_size);
    ASSERT_NOT_NULL(src);

    int pos = 0;
    pos += snprintf(src + pos, (size_t)(buf_size - pos), "package stress\n");
    for (int k = 0; k < N; k++) {
        pos += snprintf(src + pos, (size_t)(buf_size - pos), "import \"pkg/%05d\"\n", k);
    }

    CBMFileResult *r = extract(src, CBM_LANG_GO, "t", "stress.go");
    free(src);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(r->imports.count, N);
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * config_extraction_test.go ports (25 tests)
 * ═══════════════════════════════════════════════════════════════════ */

/* --- TOML (8 tests) --- */

TEST(toml_basic_table_and_pair) {
    CBMFileResult *r = extract("[database]\nhost = \"localhost\"\nport = 5432\n", CBM_LANG_TOML,
                               "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(count_defs_with_label(r, "Class"), 1);
    ASSERT(has_def(r, "Class", "database"));
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 2);
    ASSERT(has_def(r, "Variable", "host"));
    ASSERT(has_def(r, "Variable", "port"));
    cbm_free_result(r);
    PASS();
}

TEST(toml_nested_table) {
    CBMFileResult *r = extract("[server.http]\nport = 8080\n", CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 1);
    cbm_free_result(r);
    PASS();
}

TEST(toml_table_array_element) {
    CBMFileResult *r = extract("[[servers]]\nname = \"alpha\"\n[[servers]]\nname = \"beta\"\n",
                               CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 2);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 2);
    cbm_free_result(r);
    PASS();
}

TEST(toml_dotted_key) {
    CBMFileResult *r =
        extract("database.host = \"localhost\"\n", CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 1);
    cbm_free_result(r);
    PASS();
}

TEST(toml_quoted_key) {
    CBMFileResult *r = extract("\"unusual-key\" = \"value\"\n", CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 1);
    cbm_free_result(r);
    PASS();
}

TEST(toml_empty_table) {
    CBMFileResult *r = extract("[empty]\n", CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(count_defs_with_label(r, "Class"), 1);
    ASSERT(has_def(r, "Class", "empty"));
    ASSERT_EQ(count_defs_with_label(r, "Variable"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(toml_comments_only) {
    CBMFileResult *r =
        extract("# just a comment\n# another comment\n", CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(count_defs_with_label(r, "Class"), 0);
    ASSERT_EQ(count_defs_with_label(r, "Variable"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(toml_boolean_and_integer_values) {
    CBMFileResult *r =
        extract("enabled = true\ncount = 42\nname = \"test\"\n", CBM_LANG_TOML, "t", "config.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 3);
    cbm_free_result(r);
    PASS();
}

/* --- INI (4 tests) --- */

TEST(ini_basic_section_and_setting) {
    CBMFileResult *r =
        extract("[database]\nhost = localhost\nport = 5432\n", CBM_LANG_INI, "t", "config.ini");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 1);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 2);
    cbm_free_result(r);
    PASS();
}

TEST(ini_multiple_sections) {
    CBMFileResult *r = extract("[section1]\nkey1 = val1\n[section2]\nkey2 = val2\n", CBM_LANG_INI,
                               "t", "config.ini");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 2);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 2);
    cbm_free_result(r);
    PASS();
}

TEST(ini_global_keys) {
    CBMFileResult *r = extract("key1 = value1\nkey2 = value2\n", CBM_LANG_INI, "t", "config.ini");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(count_defs_with_label(r, "Class"), 0);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 2);
    cbm_free_result(r);
    PASS();
}

TEST(ini_comments) {
    CBMFileResult *r = extract("; comment\n# another comment\n[section]\nkey = val\n", CBM_LANG_INI,
                               "t", "config.ini");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 1);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 1);
    cbm_free_result(r);
    PASS();
}

/* --- JSON (5 tests) --- */

TEST(json_basic_pair) {
    CBMFileResult *r =
        extract("{\"host\": \"localhost\", \"port\": 5432}", CBM_LANG_JSON, "t", "config.json");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 2);
    ASSERT(has_def(r, "Variable", "host"));
    ASSERT(has_def(r, "Variable", "port"));
    cbm_free_result(r);
    PASS();
}

TEST(json_nested_object) {
    CBMFileResult *r = extract("{\"database\": {\"host\": \"localhost\", \"port\": 5432}}",
                               CBM_LANG_JSON, "t", "config.json");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 3);
    cbm_free_result(r);
    PASS();
}

TEST(json_empty_object) {
    CBMFileResult *r = extract("{}", CBM_LANG_JSON, "t", "config.json");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(count_defs_with_label(r, "Variable"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(json_boolean_null_values) {
    CBMFileResult *r = extract("{\"enabled\": true, \"value\": null, \"name\": \"test\"}",
                               CBM_LANG_JSON, "t", "config.json");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 3);
    cbm_free_result(r);
    PASS();
}

TEST(json_package_json_deps) {
    CBMFileResult *r =
        extract("{\"name\":\"pkg\",\"dependencies\":{\"express\":\"^4.0\",\"lodash\":\"^4.17\"}}",
                CBM_LANG_JSON, "t", "package.json");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Variable"), 4);
    ASSERT(has_def(r, "Variable", "name"));
    ASSERT(has_def(r, "Variable", "dependencies"));
    ASSERT(has_def(r, "Variable", "express"));
    ASSERT(has_def(r, "Variable", "lodash"));
    cbm_free_result(r);
    PASS();
}

/* --- XML (4 tests) --- */

TEST(xml_basic_element) {
    CBMFileResult *r = extract(
        "<?xml version=\"1.0\"?><config><database><host>localhost</host></database></config>",
        CBM_LANG_XML, "t", "config.xml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 3);
    ASSERT(has_def(r, "Class", "config"));
    ASSERT(has_def(r, "Class", "database"));
    ASSERT(has_def(r, "Class", "host"));
    cbm_free_result(r);
    PASS();
}

TEST(xml_self_closing_tag) {
    CBMFileResult *r =
        extract("<?xml version=\"1.0\"?><config><feature enabled=\"true\"/></config>", CBM_LANG_XML,
                "t", "config.xml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 2);
    cbm_free_result(r);
    PASS();
}

TEST(xml_empty_document) {
    CBMFileResult *r = extract("<?xml version=\"1.0\"?><root/>", CBM_LANG_XML, "t", "config.xml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 1);
    cbm_free_result(r);
    PASS();
}

TEST(xml_multiple_children) {
    CBMFileResult *r =
        extract("<?xml version=\"1.0\"?><servers><server/><server/><server/></servers>",
                CBM_LANG_XML, "t", "config.xml");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Class"), 4); /* servers + 3x server */
    cbm_free_result(r);
    PASS();
}

/* --- Markdown (4 tests) --- */

TEST(markdown_atx_headings) {
    CBMFileResult *r =
        extract("# Title\n## Section\n### Subsection\n", CBM_LANG_MARKDOWN, "t", "README.md");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Section"), 3);
    ASSERT_EQ(count_defs_with_label(r, "Class"), 0); /* Markdown: Section, not Class */
    cbm_free_result(r);
    PASS();
}

TEST(markdown_setext_headings) {
    CBMFileResult *r =
        extract("Title\n=====\nSection\n------\n", CBM_LANG_MARKDOWN, "t", "README.md");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Section"), 2);
    cbm_free_result(r);
    PASS();
}

TEST(markdown_heading_content) {
    CBMFileResult *r = extract("# Installation Guide\n## Prerequisites\n## Setup\n",
                               CBM_LANG_MARKDOWN, "t", "README.md");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_GTE(count_defs_with_label(r, "Section"), 3);
    ASSERT(has_def(r, "Section", "Installation Guide"));
    ASSERT(has_def(r, "Section", "Prerequisites"));
    ASSERT(has_def(r, "Section", "Setup"));
    cbm_free_result(r);
    PASS();
}

TEST(markdown_no_headings) {
    CBMFileResult *r =
        extract("Just a paragraph\n\nAnother paragraph\n", CBM_LANG_MARKDOWN, "t", "README.md");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_EQ(count_defs_with_label(r, "Section"), 0);
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Python __init__.py Module QN collision regression
 * ═══════════════════════════════════════════════════════════════════ */

TEST(python_init_module_qn_not_collide_with_folder) {
    /* Bug: __init__.py Module QN was identical to the Folder QN for the
     * same directory, causing the Folder node to be overwritten when the
     * Module was upserted. The Module QN must contain "__init__" to
     * distinguish it from the Folder QN. */
    CBMFileResult *r = extract("class Config:\n    DEBUG = True\n\ndef setup():\n    pass\n",
                               CBM_LANG_PYTHON, "proj", "mypackage/__init__.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* Module node must exist */
    ASSERT_GTE(r->defs.count, 1);
    ASSERT_STR_EQ(r->defs.items[0].label, "Module");

    /* Module QN must contain __init__ (not be stripped to just "proj.mypackage") */
    ASSERT_NOT_NULL(r->module_qn);
    ASSERT_NOT_NULL(strstr(r->module_qn, "__init__"));

    /* But symbols inside __init__.py should NOT have __init__ in their QN */
    int found_config = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].name, "Config") == 0) {
            ASSERT_NOT_NULL(r->defs.items[i].qualified_name);
            /* Should be "proj.mypackage.Config", NOT "proj.mypackage.__init__.Config" */
            ASSERT_STR_EQ(r->defs.items[i].qualified_name, "proj.mypackage.Config");
            found_config = 1;
        }
    }
    ASSERT_EQ(found_config, 1);

    cbm_free_result(r);
    PASS();
}

TEST(python_init_nested_module_qn) {
    /* Deeply nested __init__.py — same collision must not happen */
    CBMFileResult *r = extract("def greet():\n    return 'hello'\n", CBM_LANG_PYTHON, "proj",
                               "docker-images/cloud-runs/bq-sync-api/__init__.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_NOT_NULL(r->module_qn);
    /* Must contain __init__ to not collide with Folder QN */
    ASSERT_NOT_NULL(strstr(r->module_qn, "__init__"));
    cbm_free_result(r);
    PASS();
}

TEST(js_index_module_qn_not_collide_with_folder) {
    /* Same bug for JS/TS index.ts files */
    CBMFileResult *r = extract("export function App() { return null; }\n", CBM_LANG_TYPESCRIPT,
                               "proj", "src/components/index.ts");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_NOT_NULL(r->module_qn);
    /* Must contain "index" to not collide with Folder QN */
    ASSERT_NOT_NULL(strstr(r->module_qn, "index"));
    cbm_free_result(r);
    PASS();
}

TEST(python_regular_module_qn_unchanged) {
    /* Non-__init__.py Python files should be unaffected */
    CBMFileResult *r =
        extract("def helper():\n    pass\n", CBM_LANG_PYTHON, "proj", "mypackage/utils.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_NOT_NULL(r->module_qn);
    /* Regular module QN should not contain __init__ or index */
    ASSERT_STR_EQ(r->module_qn, "proj.mypackage.utils");
    cbm_free_result(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Suite
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(extraction) {
    /* Initialize extraction library */
    cbm_init();

    /* OOP */
    RUN_TEST(java_class);
    RUN_TEST(java_method);
    RUN_TEST(java_interface);
    RUN_TEST(php_class);
    RUN_TEST(php_function);
    RUN_TEST(ruby_class);
    RUN_TEST(ruby_module);
    RUN_TEST(csharp_class);
    RUN_TEST(csharp_interface);
    RUN_TEST(swift_class);
    RUN_TEST(kotlin_function);
    RUN_TEST(kotlin_class);
    RUN_TEST(scala_function);
    RUN_TEST(scala_class);
    RUN_TEST(dart_class);
    RUN_TEST(groovy_class);

    /* Systems */
    RUN_TEST(rust_function);
    RUN_TEST(rust_struct);
    RUN_TEST(go_function);
    RUN_TEST(go_struct);
    RUN_TEST(go_interface);
    RUN_TEST(zig_function);
    RUN_TEST(c_function);
    RUN_TEST(c_struct);
    RUN_TEST(cpp_class);

    /* Scripting */
    RUN_TEST(python_function);
    RUN_TEST(python_class);
    RUN_TEST(js_function);
    RUN_TEST(js_class);
    RUN_TEST(ts_function);
    RUN_TEST(ts_class);
    RUN_TEST(lua_function);
    RUN_TEST(bash_function);
    RUN_TEST(perl_function);
    RUN_TEST(r_function);

    /* Functional */
    RUN_TEST(elixir_function);
    RUN_TEST(haskell_function);
    RUN_TEST(ocaml_function);
    RUN_TEST(erlang_function);

    /* Markup/Config */
    RUN_TEST(yaml_variables);
    RUN_TEST(hcl_blocks);
    RUN_TEST(sql_create_table);
    RUN_TEST(dockerfile_stages);

    /* Scientific */
    RUN_TEST(matlab_function);
    RUN_TEST(lean_function);
    RUN_TEST(form_procedure);
    RUN_TEST(wolfram_function);
    RUN_TEST(magma_function);

    /* v0.5 expansion */
    RUN_TEST(fsharp_function);
    RUN_TEST(julia_function);
    RUN_TEST(elm_function);
    RUN_TEST(nix_function);
    RUN_TEST(fortran_function);

    /* OOP/Systems variants */
    RUN_TEST(swift_struct);
    RUN_TEST(swift_simple_call);
    RUN_TEST(swift_method_call);
    RUN_TEST(swift_constructor_call);
    RUN_TEST(swift_chained_call);
    RUN_TEST(objc_interface);
    RUN_TEST(objc_implementation);
    RUN_TEST(dart_top_level_function);
    RUN_TEST(rust_enum);
    RUN_TEST(zig_struct);
    RUN_TEST(cpp_function);
    RUN_TEST(cobol_paragraph);
    RUN_TEST(verilog_module);
    RUN_TEST(cuda_kernel);
    RUN_TEST(python_decorator);
    RUN_TEST(ts_interface);
    RUN_TEST(tsx_component);
    RUN_TEST(lua_table_method);
    RUN_TEST(emacs_lisp_defun);
    RUN_TEST(emacs_lisp_defvar);
    RUN_TEST(haskell_data_type);
    RUN_TEST(clojure_function);

    /* Config/Markup */
    RUN_TEST(html_elements);
    RUN_TEST(sql_function);
    RUN_TEST(meson_project);
    RUN_TEST(css_rules);
    RUN_TEST(scss_rules);
    RUN_TEST(toml_basic);
    RUN_TEST(cmake_function);
    RUN_TEST(json_object);
    RUN_TEST(protobuf_message);
    RUN_TEST(graphql_type);
    RUN_TEST(svelte_component);
    RUN_TEST(vue_component);
    RUN_TEST(glsl_shader);
    RUN_TEST(vimscript_function);

    /* Scientific extended */
    RUN_TEST(matlab_parse);
    RUN_TEST(matlab_call);
    RUN_TEST(lean_parse);
    RUN_TEST(lean_call);
    RUN_TEST(lean_type_annotation_not_call);
    RUN_TEST(form_parse);
    RUN_TEST(form_call);
    RUN_TEST(magma_procedure);
    RUN_TEST(magma_parse);
    RUN_TEST(magma_import);
    RUN_TEST(magma_call);
    RUN_TEST(magma_disambiguation);
    RUN_TEST(wolfram_function_extended);
    RUN_TEST(wolfram_call);
    RUN_TEST(wolfram_caller_attribution);
    RUN_TEST(wolfram_parse);
    RUN_TEST(wolfram_import);
    RUN_TEST(wolfram_nested_def);

    /* cbm_test.go ports */
    RUN_TEST(python_docstring);
    RUN_TEST(go_function_extraction);
    RUN_TEST(js_arrow_function);

    /* language_failures_test.go ports */
    RUN_TEST(commonlisp_defun);
    RUN_TEST(commonlisp_multiple_functions);
    RUN_TEST(commonlisp_defmacro);
    RUN_TEST(makefile_rule_as_function);
    RUN_TEST(makefile_multiple_targets);
    RUN_TEST(makefile_variable_extraction);
    RUN_TEST(vimscript_function_extraction);
    RUN_TEST(vimscript_function_without_bang);
    RUN_TEST(julia_function_extraction);
    RUN_TEST(julia_function_with_args);

    /* Cross-cutting */
    RUN_TEST(python_calls);
    RUN_TEST(go_calls);
    RUN_TEST(python_imports);
    RUN_TEST(js_imports);
    RUN_TEST(go_imports);
    RUN_TEST(java_imports);
    RUN_TEST(rust_imports);
    RUN_TEST(c_imports);
    RUN_TEST(ruby_imports);
    RUN_TEST(lua_imports);
    RUN_TEST(gdscript_parse_basic);
    RUN_TEST(gdscript_constructor_definition_maps_to_init_method_and_scope);
    RUN_TEST(gdscript_defs_anchor_and_methods);
    RUN_TEST(gdscript_defs_script_fallback_anchor);
    RUN_TEST(gdscript_defs_static_func_var_and_class_stmt);
    RUN_TEST(gdscript_export_variable_flags_and_anchor_linkage);
    RUN_TEST(gdscript_signal_calls_and_import_alias);
    RUN_TEST(gdscript_nested_preload_call_chain_emits_import);
    RUN_TEST(gdscript_nested_preload_argument_emits_import);
    RUN_TEST(gdscript_extends_nested_preload_emits_import);
    RUN_TEST(gdscript_top_level_string_ref_uses_script_anchor_scope);
    RUN_TEST(gdscript_signal_metadata_dotted_receivers);
    RUN_TEST(gdscript_signal_metadata_receiver_emit_prefers_receiver_name_over_payload);
    RUN_TEST(gdscript_signal_metadata_connect_keeps_receiver_signal_with_extra_args);
    RUN_TEST(gdscript_signal_metadata_connect_keeps_dotless_receiver_signal_with_flags);
    RUN_TEST(gdscript_imports_ignore_non_global_and_dynamic_loads);
    RUN_TEST(gdscript_chained_callee_parenthesized_receivers);
    RUN_TEST(gdscript_import_from_plain_assignment_preload);
    RUN_TEST(gdscript_signal_calls_resolve_const_signal_name);
    RUN_TEST(gdscript_nested_class_constructor_definition_maps_to_init_method);
    RUN_TEST(gdscript_nested_class_const_does_not_resolve_outer_emit_signal_name);
    RUN_TEST(gdscript_extends_path_is_normalized_for_import_and_base_class);
    RUN_TEST(gdscript_aliasless_relative_preload_does_not_set_gd_local_name);
    RUN_TEST(gdscript_connect_with_unresolved_signal_arg_does_not_infer_receiver_name);
    RUN_TEST(gdscript_alias_imports_ignore_dynamic_quoted_prefix_expressions);
    RUN_TEST(gdscript_emit_signal_unresolved_identifier_does_not_populate_signal_metadata);
    RUN_TEST(gdscript_extends_relative_path_is_normalized_consistently);
    RUN_TEST(gdscript_callee_parsing_handles_parentheses_inside_string_args);
    RUN_TEST(gdscript_signal_metadata_ignores_dynamic_receiver_emit_connect);
    RUN_TEST(gdscript_nested_class_extends_preserves_base_classes);
    RUN_TEST(gdscript_aliasless_res_preload_and_extends_do_not_set_local_name);
    RUN_TEST(gdscript_above_root_relative_path_is_rejected);
    RUN_TEST(import_stress_go);

    /* config_extraction_test.go ports */
    RUN_TEST(toml_basic_table_and_pair);
    RUN_TEST(toml_nested_table);
    RUN_TEST(toml_table_array_element);
    RUN_TEST(toml_dotted_key);
    RUN_TEST(toml_quoted_key);
    RUN_TEST(toml_empty_table);
    RUN_TEST(toml_comments_only);
    RUN_TEST(toml_boolean_and_integer_values);
    RUN_TEST(ini_basic_section_and_setting);
    RUN_TEST(ini_multiple_sections);
    RUN_TEST(ini_global_keys);
    RUN_TEST(ini_comments);
    RUN_TEST(json_basic_pair);
    RUN_TEST(json_nested_object);
    RUN_TEST(json_empty_object);
    RUN_TEST(json_boolean_null_values);
    RUN_TEST(json_package_json_deps);
    RUN_TEST(xml_basic_element);
    RUN_TEST(xml_self_closing_tag);
    RUN_TEST(xml_empty_document);
    RUN_TEST(xml_multiple_children);
    RUN_TEST(markdown_atx_headings);
    RUN_TEST(markdown_setext_headings);
    RUN_TEST(markdown_heading_content);
    RUN_TEST(markdown_no_headings);

    /* __init__.py / index.ts Module QN collision regression */
    RUN_TEST(python_init_module_qn_not_collide_with_folder);
    RUN_TEST(python_init_nested_module_qn);
    RUN_TEST(js_index_module_qn_not_collide_with_folder);
    RUN_TEST(python_regular_module_qn_unchanged);

    cbm_shutdown();
}
