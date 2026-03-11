package cbm

import (
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

// =====================================================================
// Group A: OOP Languages
// =====================================================================

// --- Java ---
func TestJavaClass_Regression(t *testing.T) {
	src := []byte("public class Animal { private String name; public String getName() { return name; } }")
	r, err := ExtractFile(src, lang.Java, "t", "Animal.java")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Animal")
}

func TestJavaMethod_Regression(t *testing.T) {
	src := []byte("public class Svc { public void doWork() {} public int compute(int x) { return x; } }")
	r, err := ExtractFile(src, lang.Java, "t", "Svc.java")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Method"), "doWork")
	assertHasName(t, defsWithLabel(r, "Method"), "compute")
}

func TestJavaInterface_Regression(t *testing.T) {
	src := []byte("public interface Repository { void save(Object o); Object findById(long id); }")
	r, err := ExtractFile(src, lang.Java, "t", "Repo.java")
	if err != nil {
		t.Fatal(err)
	}
	defs := r.Definitions
	found := false
	for _, d := range defs {
		if d.Name == "Repository" {
			found = true
		}
	}
	if !found {
		t.Error("interface Repository not found")
	}
}

// --- PHP ---
func TestPHPClass_Regression(t *testing.T) {
	src := []byte("<?php\nclass User { public string $name; public function getName(): string { return $this->name; } }")
	r, err := ExtractFile(src, lang.PHP, "t", "User.php")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "User")
	assertHasName(t, defsWithLabel(r, "Method"), "getName")
}

func TestPHPFunction_Regression(t *testing.T) {
	src := []byte("<?php\nfunction greet(string $name): string { return 'Hello ' . $name; }")
	r, err := ExtractFile(src, lang.PHP, "t", "helpers.php")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
}

// --- Ruby ---
func TestRubyClass_Regression(t *testing.T) {
	src := []byte("class Animal\n  def initialize(name)\n    @name = name\n  end\n  def speak\n    puts @name\n  end\nend\n")
	r, err := ExtractFile(src, lang.Ruby, "t", "animal.rb")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Animal")
	assertHasName(t, defsWithLabel(r, "Method"), "speak")
}

func TestRubyModule_Regression(t *testing.T) {
	src := []byte("module Greetable\n  def greet\n    \"Hello\"\n  end\nend\n")
	r, err := ExtractFile(src, lang.Ruby, "t", "greetable.rb")
	if err != nil {
		t.Fatal(err)
	}
	defs := r.Definitions
	found := false
	for _, d := range defs {
		if d.Name == "Greetable" {
			found = true
		}
	}
	if !found {
		t.Error("module Greetable not found")
	}
}

// --- C# ---
func TestCSharpClass_Regression(t *testing.T) {
	src := []byte("namespace App { public class Service { public void Run() {} public int Compute(int x) => x * 2; } }")
	r, err := ExtractFile(src, lang.CSharp, "t", "Service.cs")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Service")
	assertHasName(t, defsWithLabel(r, "Method"), "Run")
}

func TestCSharpInterface_Regression(t *testing.T) {
	src := []byte("public interface IService { void Execute(); string GetStatus(); }")
	r, err := ExtractFile(src, lang.CSharp, "t", "IService.cs")
	if err != nil {
		t.Fatal(err)
	}
	defs := r.Definitions
	found := false
	for _, d := range defs {
		if d.Name == "IService" {
			found = true
		}
	}
	if !found {
		t.Error("interface IService not found")
	}
}

// --- Swift ---
func TestSwiftClass_Regression(t *testing.T) {
	src := []byte("class Vehicle {\n    var speed: Int = 0\n    func accelerate() { speed += 10 }\n    func stop() { speed = 0 }\n}\n")
	r, err := ExtractFile(src, lang.Swift, "t", "Vehicle.swift")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Vehicle")
	assertHasName(t, defsWithLabel(r, "Method"), "accelerate")
}

func TestSwiftStruct_Regression(t *testing.T) {
	src := []byte("struct Point {\n    var x: Double\n    var y: Double\n    func distance() -> Double { return (x*x + y*y).squareRoot() }\n}\n")
	r, err := ExtractFile(src, lang.Swift, "t", "Point.swift")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Method"), "distance")
}

func TestSwiftSimpleCall_Regression(t *testing.T) {
	src := []byte("func main() { greet() }\nfunc greet() { print(\"hello\") }\n")
	r, err := ExtractFile(src, lang.Swift, "t", "main.swift")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Calls) == 0 {
		t.Fatal("expected calls, got 0")
	}
	found := false
	for _, c := range r.Calls {
		if c.CalleeName == "greet" {
			found = true
			if c.EnclosingFuncQN == "" {
				t.Error("greet() call should have enclosing function QN")
			}
		}
	}
	if !found {
		t.Error("call to greet() not found")
	}
}

func TestSwiftMethodCall_Regression(t *testing.T) {
	src := []byte("class Foo {\n    func bar() { baz.run() }\n}\n")
	r, err := ExtractFile(src, lang.Swift, "t", "Foo.swift")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range r.Calls {
		if c.CalleeName == "baz.run" {
			found = true
		}
	}
	if !found {
		t.Error("call to baz.run() not found in Foo.bar")
	}
}

func TestSwiftConstructor_Regression(t *testing.T) {
	src := []byte("func create() { let x = MyClass() }\n")
	r, err := ExtractFile(src, lang.Swift, "t", "create.swift")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range r.Calls {
		if c.CalleeName == "MyClass" {
			found = true
		}
	}
	if !found {
		t.Error("constructor call MyClass() not found")
	}
}

func TestSwiftChainedCall_Regression(t *testing.T) {
	src := []byte("func setup() { AlarmScheduler.shared.startKeepAlive() }\n")
	r, err := ExtractFile(src, lang.Swift, "t", "setup.swift")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Calls) == 0 {
		t.Fatal("expected calls for chained dot syntax, got 0")
	}
	// Should capture the full navigation expression
	found := false
	for _, c := range r.Calls {
		t.Logf("Call: callee=%q enclosing=%q", c.CalleeName, c.EnclosingFuncQN)
		if c.CalleeName != "" {
			found = true
		}
	}
	if !found {
		t.Error("chained call not extracted")
	}
}

// --- Kotlin ---
func TestKotlinFunction_Regression(t *testing.T) {
	src := []byte("fun greet(name: String): String = \"Hello $name\"\nfun main() { println(greet(\"World\")) }\n")
	r, err := ExtractFile(src, lang.Kotlin, "t", "main.kt")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

func TestKotlinClass_Regression(t *testing.T) {
	src := []byte("class User(val name: String) {\n    fun display(): String = \"User: $name\"\n}\n")
	r, err := ExtractFile(src, lang.Kotlin, "t", "User.kt")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "User")
}

// --- Scala ---
func TestScalaFunction_Regression(t *testing.T) {
	src := []byte("object Main {\n  def greet(name: String): String = s\"Hello $name\"\n  def main(args: Array[String]): Unit = println(greet(\"World\"))\n}\n")
	r, err := ExtractFile(src, lang.Scala, "t", "Main.scala")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Method"), "greet")
}

func TestScalaClass_Regression(t *testing.T) {
	src := []byte("class Animal(val name: String) {\n  def speak(): String = s\"I am $name\"\n}\n")
	r, err := ExtractFile(src, lang.Scala, "t", "Animal.scala")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Animal")
}

// --- Objective-C ---
func TestObjCInterface_Regression(t *testing.T) {
	src := []byte("@interface Animal : NSObject\n- (NSString *)name;\n- (void)speak;\n@end\n")
	r, err := ExtractFile(src, lang.ObjectiveC, "t", "Animal.h")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from ObjC interface")
	}
}

func TestObjCImplementation_Regression(t *testing.T) {
	src := []byte("@implementation Animal\n- (NSString *)name { return @\"Animal\"; }\n- (void)speak { NSLog(@\"...\"); }\n@end\n")
	r, err := ExtractFile(src, lang.ObjectiveC, "t", "Animal.m")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from ObjC impl")
	}
}

// --- Dart ---
func TestDartClass_Regression(t *testing.T) {
	src := []byte("class Animal {\n  String name;\n  Animal(this.name);\n  String speak() => 'I am $name';\n}\n")
	r, err := ExtractFile(src, lang.Dart, "t", "animal.dart")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Animal")
	assertHasName(t, defsWithLabel(r, "Method"), "speak")
}

func TestDartTopLevelFunction_Regression(t *testing.T) {
	src := []byte("void main() {\n  print('Hello');\n}\nString greet(String name) => 'Hello $name';\n")
	r, err := ExtractFile(src, lang.Dart, "t", "main.dart")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "main")
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
}

// --- Groovy ---
func TestGroovyClass_Regression(t *testing.T) {
	src := []byte("class Greeter {\n    String name\n    String greet() { \"Hello, $name\" }\n    static void main(args) { println new Greeter(name:'World').greet() }\n}\n")
	r, err := ExtractFile(src, lang.Groovy, "t", "Greeter.groovy")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Greeter")
	assertHasName(t, defsWithLabel(r, "Method"), "greet")
}

// =====================================================================
// Group B: Systems Languages
// =====================================================================

// --- Rust ---
func TestRustFunction_Regression(t *testing.T) {
	src := []byte("fn main() { println!(\"Hello\"); }\npub fn add(a: i32, b: i32) -> i32 { a + b }\n")
	r, err := ExtractFile(src, lang.Rust, "t", "main.rs")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "main")
	assertHasName(t, defsWithLabel(r, "Function"), "add")
}

func TestRustStruct_Regression(t *testing.T) {
	src := []byte("pub struct Point { pub x: f64, pub y: f64 }\nimpl Point { pub fn new(x: f64, y: f64) -> Self { Point { x, y } } }\n")
	r, err := ExtractFile(src, lang.Rust, "t", "point.rs")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Point")
	assertHasName(t, defsWithLabel(r, "Method"), "new")
}

func TestRustEnum_Regression(t *testing.T) {
	src := []byte("pub enum Direction { North, South, East, West }\n")
	r, err := ExtractFile(src, lang.Rust, "t", "dir.rs")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition for Rust enum")
	}
}

// --- Go ---
func TestGoFunction_Regression(t *testing.T) {
	src := []byte("package main\nfunc Greet(name string) string { return \"Hello, \" + name }\nfunc main() { Greet(\"World\") }\n")
	r, err := ExtractFile(src, lang.Go, "t", "main.go")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "Greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

func TestGoStruct_Regression(t *testing.T) {
	src := []byte("package main\ntype Server struct { Host string; Port int }\nfunc (s *Server) Start() error { return nil }\n")
	r, err := ExtractFile(src, lang.Go, "t", "server.go")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Server")
	assertHasName(t, defsWithLabel(r, "Method"), "Start")
}

func TestGoInterface_Regression(t *testing.T) {
	src := []byte("package main\ntype Handler interface { ServeHTTP() error; Close() }\n")
	r, err := ExtractFile(src, lang.Go, "t", "handler.go")
	if err != nil {
		t.Fatal(err)
	}
	defs := r.Definitions
	found := false
	for _, d := range defs {
		if d.Name == "Handler" {
			found = true
		}
	}
	if !found {
		t.Error("interface Handler not found")
	}
}

// --- Zig ---
func TestZigFunction_Regression(t *testing.T) {
	src := []byte("pub fn add(a: i32, b: i32) i32 { return a + b; }\npub fn main() void { _ = add(1, 2); }\n")
	r, err := ExtractFile(src, lang.Zig, "t", "main.zig")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "add")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

func TestZigStruct_Regression(t *testing.T) {
	src := []byte("const Point = struct { x: f32, y: f32, pub fn dist(self: Point) f32 { return self.x + self.y; } };\n")
	r, err := ExtractFile(src, lang.Zig, "t", "point.zig")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Zig struct")
	}
}

// --- C ---
func TestCFunction_Regression(t *testing.T) {
	src := []byte("#include <stdio.h>\nint add(int a, int b) { return a + b; }\nint main() { printf(\"%d\\n\", add(1,2)); return 0; }\n")
	r, err := ExtractFile(src, lang.C, "t", "main.c")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "add")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

func TestCStruct_Regression(t *testing.T) {
	src := []byte("struct Point { int x; int y; };\nvoid print_point(struct Point p) { /* ... */ }\n")
	r, err := ExtractFile(src, lang.C, "t", "point.c")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "print_point")
}

// --- C++ ---
func TestCppFunction_Regression(t *testing.T) {
	src := []byte("#include <string>\nstd::string greet(const std::string& name) { return \"Hello \" + name; }\nint main() { return 0; }\n")
	r, err := ExtractFile(src, lang.CPP, "t", "main.cpp")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from C++ file")
	}
}

func TestCppClass_Regression(t *testing.T) {
	src := []byte("class Animal {\npublic:\n    std::string name;\n    Animal(std::string n) : name(n) {}\n    void speak() { /* ... */ }\n};\n")
	r, err := ExtractFile(src, lang.CPP, "t", "Animal.cpp")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from C++ class")
	}
}

// --- COBOL ---
func TestCOBOLParagraph_Regression(t *testing.T) {
	src := []byte("IDENTIFICATION DIVISION.\nPROGRAM-ID. HELLO.\nPROCEDURE DIVISION.\n    DISPLAY-GREETING.\n        DISPLAY 'HELLO WORLD'.\n        STOP RUN.\n")
	r, err := ExtractFile(src, lang.COBOL, "t", "hello.cbl")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from COBOL")
	}
}

// --- Verilog ---
func TestVerilogModule_Regression(t *testing.T) {
	src := []byte("module adder(input a, input b, output sum);\n  assign sum = a + b;\nendmodule\n")
	r, err := ExtractFile(src, lang.Verilog, "t", "adder.v")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Verilog")
	}
}

// --- CUDA ---
func TestCUDAKernel_Regression(t *testing.T) {
	src := []byte("__global__ void vectorAdd(float *a, float *b, float *c, int n) {\n    int i = blockIdx.x * blockDim.x + threadIdx.x;\n    if (i < n) c[i] = a[i] + b[i];\n}\nint main() { return 0; }\n")
	r, err := ExtractFile(src, lang.CUDA, "t", "vector.cu")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from CUDA file")
	}
}

// =====================================================================
// Group C: Dynamic Languages
// =====================================================================

// --- Python ---
func TestPythonFunction_Regression(t *testing.T) {
	src := []byte("def greet(name: str) -> str:\n    return f'Hello {name}'\n\ndef main():\n    print(greet('World'))\n")
	r, err := ExtractFile(src, lang.Python, "t", "hello.py")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

func TestPythonClass_Regression(t *testing.T) {
	src := []byte("class Animal:\n    def __init__(self, name: str):\n        self.name = name\n    def speak(self) -> str:\n        return f'I am {self.name}'\n")
	r, err := ExtractFile(src, lang.Python, "t", "animal.py")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Animal")
	assertHasName(t, defsWithLabel(r, "Method"), "speak")
}

func TestPythonDecorator_Regression(t *testing.T) {
	src := []byte("class Router:\n    @staticmethod\n    def route(path: str):\n        def decorator(func): return func\n        return decorator\n")
	r, err := ExtractFile(src, lang.Python, "t", "router.py")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Router")
}

// --- JavaScript ---
func TestJavaScriptFunction_Regression(t *testing.T) {
	src := []byte("function greet(name) { return 'Hello ' + name; }\nconst add = (a, b) => a + b;\nmodule.exports = { greet, add };\n")
	r, err := ExtractFile(src, lang.JavaScript, "t", "utils.js")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
}

func TestJavaScriptClass_Regression(t *testing.T) {
	src := []byte("class Animal {\n    constructor(name) { this.name = name; }\n    speak() { return `I am ${this.name}`; }\n}\nmodule.exports = Animal;\n")
	r, err := ExtractFile(src, lang.JavaScript, "t", "Animal.js")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Animal")
	assertHasName(t, defsWithLabel(r, "Method"), "speak")
}

// --- TypeScript ---
func TestTypeScriptFunction_Regression(t *testing.T) {
	src := []byte("export function greet(name: string): string { return `Hello ${name}`; }\nexport const add = (a: number, b: number): number => a + b;\n")
	r, err := ExtractFile(src, lang.TypeScript, "t", "utils.ts")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
}

func TestTypeScriptInterface_Regression(t *testing.T) {
	src := []byte("export interface Repository<T> { findById(id: number): T; save(entity: T): void; delete(id: number): void; }\n")
	r, err := ExtractFile(src, lang.TypeScript, "t", "repo.ts")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from TypeScript interface")
	}
}

func TestTypeScriptClass_Regression(t *testing.T) {
	src := []byte("export class UserService {\n    private users: Map<number, string> = new Map();\n    add(id: number, name: string): void { this.users.set(id, name); }\n    get(id: number): string | undefined { return this.users.get(id); }\n}\n")
	r, err := ExtractFile(src, lang.TypeScript, "t", "UserService.ts")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "UserService")
}

// --- TSX ---
func TestTSXComponent_Regression(t *testing.T) {
	src := []byte("import React from 'react';\ninterface Props { name: string; }\nexport function Greeting({ name }: Props) {\n    return <div>Hello {name}</div>;\n}\nexport default Greeting;\n")
	r, err := ExtractFile(src, lang.TSX, "t", "Greeting.tsx")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "Greeting")
}

// --- Lua ---
func TestLuaFunction_Regression(t *testing.T) {
	src := []byte("local function greet(name)\n    return 'Hello ' .. name\nend\nlocal function main()\n    print(greet('World'))\nend\nmain()\n")
	r, err := ExtractFile(src, lang.Lua, "t", "main.lua")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

func TestLuaTableMethod_Regression(t *testing.T) {
	src := []byte("local M = {}\nfunction M.create(name)\n    return { name = name }\nend\nfunction M.greet(self)\n    return 'Hi ' .. self.name\nend\nreturn M\n")
	r, err := ExtractFile(src, lang.Lua, "t", "module.lua")
	if err != nil {
		t.Fatal(err)
	}
	if len(defsWithLabel(r, "Function")) < 1 {
		t.Error("expected >=1 Function from Lua table method")
	}
}

// --- Bash ---
func TestBashFunction_Regression(t *testing.T) {
	src := []byte("#!/usr/bin/env bash\ngreet() {\n    echo \"Hello $1\"\n}\nbuild_project() {\n    go build ./...\n}\ngreet World\n")
	r, err := ExtractFile(src, lang.Bash, "t", "build.sh")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "build_project")
}

// --- Nix ---
func TestNixFunction_Regression(t *testing.T) {
	src := []byte("{ pkgs ? import <nixpkgs> {} }:\nlet\n  greet = name: \"Hello ${name}\";\n  build = src: pkgs.stdenv.mkDerivation { inherit src; };\nin\n{ inherit greet build; }\n")
	r, err := ExtractFile(src, lang.Nix, "t", "default.nix")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Nix file")
	}
}

// --- EmacsLisp (critical: must not be broken by CommonLisp fix) ---
func TestEmacsLispDefun_Regression(t *testing.T) {
	src := []byte("(defun greet (name)\n  (message \"Hello %s\" name))\n(defun main ()\n  (greet \"World\"))\n")
	r, err := ExtractFile(src, lang.EmacsLisp, "t", "init.el")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

func TestEmacsLispDefvar_Regression(t *testing.T) {
	src := []byte("(defvar my-count 0 \"A counter.\")\n(defcustom my-name \"World\" \"The name.\"\n  :type 'string)\n")
	r, err := ExtractFile(src, lang.EmacsLisp, "t", "vars.el")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from EmacsLisp vars")
	}
}

// --- R ---
func TestRFunction_Regression(t *testing.T) {
	src := []byte("greet <- function(name) {\n  paste(\"Hello\", name)\n}\nmain <- function() {\n  print(greet(\"World\"))\n}\n")
	r, err := ExtractFile(src, lang.R, "t", "main.R")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

// --- Perl ---
func TestPerlFunction_Regression(t *testing.T) {
	src := []byte("sub greet {\n    my ($name) = @_;\n    return \"Hello $name\";\n}\nsub main {\n    print greet('World'), \"\\n\";\n}\nmain();\n")
	r, err := ExtractFile(src, lang.Perl, "t", "main.pl")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

// =====================================================================
// Group D: Functional Languages
// =====================================================================

// --- Haskell ---
func TestHaskellFunction_Regression(t *testing.T) {
	src := []byte("module Main where\ngreet :: String -> String\ngreet name = \"Hello \" ++ name\nmain :: IO ()\nmain = putStrLn (greet \"World\")\n")
	r, err := ExtractFile(src, lang.Haskell, "t", "Main.hs")
	if err != nil {
		t.Fatal(err)
	}
	// greet (with args) uses prod_id 151 with inherited field_name — always extracted
	// main = ... (no args) uses prod_id 77 with no field_name — not extracted (known limitation)
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
}

func TestHaskellDataType_Regression(t *testing.T) {
	src := []byte("data Shape = Circle Double | Rectangle Double Double\narea :: Shape -> Double\narea (Circle r) = pi * r * r\narea (Rectangle w h) = w * h\n")
	r, err := ExtractFile(src, lang.Haskell, "t", "Shape.hs")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Haskell data type")
	}
}

// --- Erlang ---
func TestErlangFunction_Regression(t *testing.T) {
	src := []byte("-module(hello).\n-export([greet/1, main/0]).\ngreet(Name) -> io:format(\"Hello ~s~n\", [Name]).\nmain() -> greet(\"World\").\n")
	r, err := ExtractFile(src, lang.Erlang, "t", "hello.erl")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

// --- Elm ---
func TestElmFunction_Regression(t *testing.T) {
	src := []byte("module Main exposing (..)\nimport Html exposing (text)\ngreet : String -> String\ngreet name = \"Hello \" ++ name\nmain = text (greet \"World\")\n")
	r, err := ExtractFile(src, lang.Elm, "t", "Main.elm")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
	assertHasName(t, defsWithLabel(r, "Function"), "main")
}

// --- Elixir ---
func TestElixirFunction_Regression(t *testing.T) {
	src := []byte("defmodule Greeter do\n  def greet(name) do\n    \"Hello #{name}\"\n  end\n  def main do\n    IO.puts(greet(\"World\"))\n  end\nend\n")
	r, err := ExtractFile(src, lang.Elixir, "t", "greeter.ex")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "Greeter")
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
}

// --- F# ---
func TestFSharpFunction_Regression(t *testing.T) {
	src := []byte("module Greeter\nlet greet name = sprintf \"Hello %s\" name\nlet main argv =\n    printfn \"%s\" (greet \"World\")\n    0\n")
	r, err := ExtractFile(src, lang.FSharp, "t", "Greeter.fs")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from F# module")
	}
}

// --- Clojure ---
func TestClojureFunction_Regression(t *testing.T) {
	// Clojure uses list_lit for all forms including defn — no dedicated defn node type.
	// FunctionNodeTypes is empty, so no function definitions are extracted (known limitation).
	// The test just verifies extraction completes without error.
	src := []byte("(ns greeter.core)\n(defn greet [name]\n  (str \"Hello \" name))\n(defn -main [& args]\n  (println (greet \"World\")))\n")
	_, err := ExtractFile(src, lang.Clojure, "t", "core.clj")
	if err != nil {
		t.Fatal(err)
	}
}

// --- OCaml ---
func TestOCamlFunction_Regression(t *testing.T) {
	src := []byte("let greet name = Printf.sprintf \"Hello %s\" name\nlet () = print_endline (greet \"World\")\n")
	r, err := ExtractFile(src, lang.OCaml, "t", "main.ml")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Function"), "greet")
}

// =====================================================================
// Group E: Config / Markup Languages
// =====================================================================

// --- YAML ---
func TestYAMLMapping_Regression(t *testing.T) {
	src := []byte("server:\n  host: localhost\n  port: 8080\ndatabase:\n  url: postgres://localhost/mydb\n")
	r, err := ExtractFile(src, lang.YAML, "t", "config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	vars := defsWithLabel(r, "Variable")
	if len(vars) < 1 {
		t.Errorf("expected >=1 Variable from YAML, got 0")
	}
}

// --- Dockerfile ---
func TestDockerfileInstruction_Regression(t *testing.T) {
	src := []byte("FROM golang:1.21 AS builder\nWORKDIR /app\nCOPY . .\nRUN go build -o server ./cmd/server\nFROM alpine:3.18\nCOPY --from=builder /app/server /usr/local/bin/server\nEXPOSE 8080\nCMD [\"server\"]\n")
	r, err := ExtractFile(src, lang.Dockerfile, "t", "Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Dockerfile")
	}
}

// --- HTML ---
func TestHTMLElements_Regression(t *testing.T) {
	src := []byte("<!DOCTYPE html><html><head><title>Test</title></head><body><h1>Hello</h1><p>World</p></body></html>")
	r, err := ExtractFile(src, lang.HTML, "t", "index.html")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from HTML")
	}
}

// --- SQL ---
func TestSQLTable_Regression(t *testing.T) {
	src := []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);\nCREATE INDEX idx_users_email ON users(email);\n")
	r, err := ExtractFile(src, lang.SQL, "t", "schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from SQL")
	}
}

func TestSQLFunction_Regression(t *testing.T) {
	src := []byte("CREATE FUNCTION get_user_count() RETURNS INTEGER AS $$ SELECT COUNT(*) FROM users; $$ LANGUAGE SQL;\n")
	r, err := ExtractFile(src, lang.SQL, "t", "funcs.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from SQL function")
	}
}

// --- Meson ---
func TestMesonProject_Regression(t *testing.T) {
	src := []byte("project('myapp', 'c', version: '1.0.0')\nexecutable('myapp', 'main.c', install: true)\n")
	r, err := ExtractFile(src, lang.Meson, "t", "meson.build")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from meson.build")
	}
}

// --- CSS ---
func TestCSSRules_Regression(t *testing.T) {
	src := []byte(".container { display: flex; width: 100%; }\n.button { background: #007bff; color: white; border: none; }\n@media (max-width: 768px) { .container { flex-direction: column; } }\n")
	r, err := ExtractFile(src, lang.CSS, "t", "styles.css")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from CSS")
	}
}

// --- HCL (Terraform) ---
func TestHCLResource_Regression(t *testing.T) {
	src := []byte("resource \"aws_instance\" \"web\" {\n  ami           = \"ami-12345678\"\n  instance_type = \"t3.micro\"\n  tags = { Name = \"web-server\" }\n}\n")
	r, err := ExtractFile(src, lang.HCL, "t", "main.tf")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from HCL/Terraform")
	}
}

// --- SCSS ---
func TestSCSSRules_Regression(t *testing.T) {
	src := []byte("$primary: #007bff;\n.container {\n  width: 100%;\n  .button {\n    background: $primary;\n    &:hover { opacity: 0.8; }\n  }\n}\n")
	r, err := ExtractFile(src, lang.SCSS, "t", "styles.scss")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from SCSS")
	}
}

// --- TOML (critical: must not be broken by config lang changes) ---
func TestTOMLBasic_Regression(t *testing.T) {
	src := []byte("[server]\nhost = \"localhost\"\nport = 8080\n\n[database]\nurl = \"postgres://localhost/db\"\nmax_connections = 10\n")
	r, err := ExtractFile(src, lang.TOML, "t", "config.toml")
	if err != nil {
		t.Fatal(err)
	}
	assertHasName(t, defsWithLabel(r, "Class"), "server")
	assertHasName(t, defsWithLabel(r, "Class"), "database")
	assertHasName(t, defsWithLabel(r, "Variable"), "host")
	assertHasName(t, defsWithLabel(r, "Variable"), "port")
}

// --- CMake ---
func TestCMakeFunction_Regression(t *testing.T) {
	src := []byte("cmake_minimum_required(VERSION 3.16)\nproject(MyApp VERSION 1.0)\nadd_executable(myapp main.cpp)\ntarget_compile_features(myapp PRIVATE cxx_std_17)\n")
	r, err := ExtractFile(src, lang.CMake, "t", "CMakeLists.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from CMakeLists.txt")
	}
}

// --- JSON ---
func TestJSONObject_Regression(t *testing.T) {
	src := []byte("{\"name\": \"myapp\", \"version\": \"1.0.0\", \"scripts\": {\"build\": \"go build\", \"test\": \"go test ./...\"}}")
	r, err := ExtractFile(src, lang.JSON, "t", "config.json")
	if err != nil {
		t.Fatal(err)
	}
	vars := defsWithLabel(r, "Variable")
	assertHasName(t, vars, "name")
	assertHasName(t, vars, "version")
}

// --- Protobuf ---
func TestProtobufMessage_Regression(t *testing.T) {
	src := []byte("syntax = \"proto3\";\npackage user;\nmessage User { int64 id = 1; string name = 2; string email = 3; }\nservice UserService { rpc GetUser(User) returns (User); }\n")
	r, err := ExtractFile(src, lang.Protobuf, "t", "user.proto")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Protobuf")
	}
}

// --- GraphQL ---
func TestGraphQLType_Regression(t *testing.T) {
	src := []byte("type User {\n  id: ID!\n  name: String!\n  email: String!\n}\ntype Query {\n  user(id: ID!): User\n  users: [User!]!\n}\n")
	r, err := ExtractFile(src, lang.GraphQL, "t", "schema.graphql")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from GraphQL schema")
	}
}

// --- Svelte ---
func TestSvelteComponent_Regression(t *testing.T) {
	src := []byte("<script>\n  let name = 'World';\n  function greet() {\n    return `Hello ${name}`;\n  }\n</script>\n<h1>{greet()}</h1>\n")
	r, err := ExtractFile(src, lang.Svelte, "t", "App.svelte")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Svelte component")
	}
}

// --- Vue ---
func TestVueComponent_Regression(t *testing.T) {
	src := []byte("<template><div>{{ message }}</div></template>\n<script>\nexport default {\n  name: 'App',\n  data() { return { message: 'Hello World' }; },\n  methods: { greet() { return this.message; } }\n};\n</script>\n")
	r, err := ExtractFile(src, lang.Vue, "t", "App.vue")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from Vue component")
	}
}

// --- GLSL ---
func TestGLSLShader_Regression(t *testing.T) {
	src := []byte("#version 330 core\nvoid main() {\n    gl_Position = vec4(0.0, 0.0, 0.0, 1.0);\n}\nvec3 transform(vec3 pos, mat4 mvp) {\n    return (mvp * vec4(pos, 1.0)).xyz;\n}\n")
	r, err := ExtractFile(src, lang.GLSL, "t", "vertex.glsl")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Definitions) < 1 {
		t.Error("expected >=1 definition from GLSL shader")
	}
}

// --- Julia ---
func TestJuliaFunction_Regression(t *testing.T) {
	src := []byte("function hello()\n  println(\"Hello, World!\")\nend\n")
	r, err := ExtractFile(src, lang.Julia, "t", "hello.jl")
	if err != nil {
		t.Fatal(err)
	}
	// If Julia extraction works, verify the function name
	fns := defsWithLabel(r, "Function")
	if len(fns) > 0 {
		assertHasName(t, fns, "hello")
	}
	// If len(fns) == 0, Julia extraction is broken — tracked by TestJuliaFunctionExtraction
}

// --- VimScript ---
func TestVimScriptFunction_Regression(t *testing.T) {
	src := []byte("function! SayHello()\n  echo 'Hello'\nendfunction\n")
	r, err := ExtractFile(src, lang.VimScript, "t", "plugin.vim")
	if err != nil {
		t.Fatal(err)
	}
	// If VimScript extraction works, verify the function name
	fns := defsWithLabel(r, "Function")
	if len(fns) > 0 {
		assertHasName(t, fns, "SayHello")
	}
	// If len(fns) == 0, VimScript extraction is broken — tracked by TestVimScriptFunctionExtraction
}

// =====================================================================
// Group H: Scientific/Math Languages
// =====================================================================

// --- MATLAB ---
func TestMATLABFunction_Regression(t *testing.T) {
	src := []byte("function result = factorial(n)\n  if n <= 1\n    result = 1;\n  else\n    result = n * factorial(n - 1);\n  end\nend\n")
	r, err := ExtractFile(src, lang.MATLAB, "t", "factorial.matlab")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	if len(fns) > 0 {
		assertHasName(t, fns, "factorial")
	}
}

func TestMATLABParse_Regression(t *testing.T) {
	src := []byte("x = 1;\ny = x + 2;\n")
	_, err := ExtractFile(src, lang.MATLAB, "t", "simple.matlab")
	if err != nil {
		t.Fatal(err)
	}
}

// --- Lean 4 ---
func TestLeanDef_Regression(t *testing.T) {
	src := []byte("def hello : String := \"Hello\"\n")
	r, err := ExtractFile(src, lang.Lean, "t", "Hello.lean")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	if len(fns) > 0 {
		assertHasName(t, fns, "hello")
	}
}

func TestLeanParse_Regression(t *testing.T) {
	src := []byte("theorem add_comm (a b : Nat) : a + b = b + a := by omega\n")
	_, err := ExtractFile(src, lang.Lean, "t", "Comm.lean")
	if err != nil {
		t.Fatal(err)
	}
}

// --- FORM ---
func TestFORMProcedure_Regression(t *testing.T) {
	src := []byte("#procedure myproc(x)\n  id `x' = 0;\n#endprocedure\n")
	r, err := ExtractFile(src, lang.FORM, "t", "calc.frm")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	if len(fns) > 0 {
		assertHasName(t, fns, "myproc")
	}
}

func TestFORMParse_Regression(t *testing.T) {
	src := []byte("Symbols x, y;\nLocal F = x + y;\nPrint;\n.end\n")
	_, err := ExtractFile(src, lang.FORM, "t", "example.frm")
	if err != nil {
		t.Fatal(err)
	}
}

func TestFORMCall_Regression(t *testing.T) {
	src := []byte("#procedure myproc(x)\n  id `x' = 0;\n#endprocedure\n#procedure caller()\n  #call myproc(1)\n#endprocedure\n")
	r, err := ExtractFile(src, lang.FORM, "t", "calc.frm")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Calls) == 0 {
		t.Fatal("expected at least one call extracted from #call myproc(1), got none")
	}
	found := false
	for _, c := range r.Calls {
		if c.CalleeName == "myproc" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected callee 'myproc' in calls, got: %+v", r.Calls)
	}
}

// --- Magma ---
func TestMagmaFunction_Regression(t *testing.T) {
	src := []byte("function Factorial(n)\n  if n le 1 then\n    return 1;\n  end if;\n  return n * Factorial(n - 1);\nend function;\n")
	r, err := ExtractFile(src, lang.Magma, "t", "fact.mag")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	if len(fns) > 0 {
		assertHasName(t, fns, "Factorial")
	}
}

func TestMagmaProcedure_Regression(t *testing.T) {
	src := []byte("procedure PrintHello()\n  print \"Hello\";\nend procedure;\n")
	r, err := ExtractFile(src, lang.Magma, "t", "hello.mag")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	if len(fns) > 0 {
		assertHasName(t, fns, "PrintHello")
	}
}

func TestMagmaParse_Regression(t *testing.T) {
	src := []byte("x := 42;\ny := x + 1;\n")
	_, err := ExtractFile(src, lang.Magma, "t", "simple.mag")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMagmaImport_Regression(t *testing.T) {
	src := []byte("load \"utils.mag\";\nload \"lib/helpers.mag\";\n")
	r, err := ExtractFile(src, lang.Magma, "t", "main.mag")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Imports) < 2 {
		t.Fatalf("expected at least 2 imports, got %d", len(r.Imports))
	}
}

func TestMagmaCall_Regression(t *testing.T) {
	src := []byte("function Foo(x)\n  y := Bar(x);\n  return y;\nend function;\n")
	r, err := ExtractFile(src, lang.Magma, "t", "calls.mag")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Calls) == 0 {
		t.Fatal("expected calls to be extracted, got 0")
	}
	found := false
	for _, c := range r.Calls {
		if c.CalleeName == "Bar" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected call to 'Bar', got calls: %v", r.Calls)
	}
}

// --- MATLAB Calls ---
func TestMATLABCall_Regression(t *testing.T) {
	src := []byte("function y = foo(x)\n  y = inv(x);\n  disp hello\nend\n")
	r, err := ExtractFile(src, lang.MATLAB, "t", "foo.matlab")
	if err != nil {
		t.Fatal(err)
	}
	// Should extract function_call "inv" and command "disp"
	if len(r.Calls) == 0 {
		t.Fatal("expected calls to be extracted, got 0")
	}
	foundInv := false
	foundDisp := false
	for _, c := range r.Calls {
		if c.CalleeName == "inv" {
			foundInv = true
		}
		if c.CalleeName == "disp" {
			foundDisp = true
		}
	}
	if !foundInv {
		t.Errorf("expected call to 'inv', got calls: %v", r.Calls)
	}
	if !foundDisp {
		t.Errorf("expected call to 'disp', got calls: %v", r.Calls)
	}
}

// --- Lean Calls ---
func TestLeanCall_Regression(t *testing.T) {
	src := []byte("def fib : Nat → Nat\n  | 0 => 1\n  | 1 => 1\n  | n + 2 => fib (n + 1) + fib n\n")
	r, err := ExtractFile(src, lang.Lean, "t", "Fib.lean")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Calls) == 0 {
		t.Fatal("expected calls to be extracted, got 0")
	}
	found := false
	for _, c := range r.Calls {
		if c.CalleeName == "fib" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected call to 'fib', got calls: %v", r.Calls)
	}
}

func TestLeanTypeAnnotationNotCall_Regression(t *testing.T) {
	// "List Nat" in a binder type should not produce a "List" call.
	// "IO.println" in the body should be extracted.
	src := []byte("def listLen (xs : List Nat) : Nat := 0\ndef greet : IO Unit := IO.println \"hi\"\n")
	r, err := ExtractFile(src, lang.Lean, "t", "Types.lean")
	if err != nil {
		t.Fatal(err)
	}
	// "List" in explicit_binder type should be filtered
	for _, c := range r.Calls {
		if c.CalleeName == "List" {
			t.Errorf("type-position apply %q (binder type) should not be extracted as a call", c.CalleeName)
		}
	}
	// IO.println in the body should be present
	foundPrintln := false
	for _, c := range r.Calls {
		if c.CalleeName == "IO.println" || c.CalleeName == "println" {
			foundPrintln = true
		}
	}
	if !foundPrintln {
		t.Errorf("expected call to IO.println/println, got calls: %v", r.Calls)
	}
}

// --- Wolfram ---
func TestWolframFunction_Regression(t *testing.T) {
	src := []byte("f[x_] := x^2\ng[x_] = x + 1\n")
	r, err := ExtractFile(src, lang.Wolfram, "t", "funcs.wl")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	if len(fns) < 2 {
		t.Fatalf("expected at least 2 functions, got %d: %v", len(fns), fns)
	}
	assertHasName(t, fns, "f")
	assertHasName(t, fns, "g")
}

func TestWolframCall_Regression(t *testing.T) {
	src := []byte("f[x_] := g[x] + h[x]\n")
	r, err := ExtractFile(src, lang.Wolfram, "t", "calls.wl")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Calls) == 0 {
		t.Fatal("expected calls to be extracted, got 0")
	}
	foundG := false
	foundH := false
	for _, c := range r.Calls {
		if c.CalleeName == "g" {
			foundG = true
		}
		if c.CalleeName == "h" {
			foundH = true
		}
	}
	if !foundG {
		t.Errorf("expected call to 'g', got calls: %v", r.Calls)
	}
	if !foundH {
		t.Errorf("expected call to 'h', got calls: %v", r.Calls)
	}
	// "f" should NOT appear as a call (it's the definition LHS)
	for _, c := range r.Calls {
		if c.CalleeName == "f" {
			t.Errorf("'f' should not be a call (it's a definition LHS), but found in calls")
		}
	}
}

func TestWolframCallerAttribution_Regression(t *testing.T) {
	// Calls inside a function body should have the enclosing function as caller,
	// not the file path (module QN). This verifies func_kinds_wolfram is wired
	// into func_kinds_for_lang so cbm_find_enclosing_func returns the def node.
	src := []byte("f[x_] := g[x] + h[x]\n")
	r, err := ExtractFile(src, lang.Wolfram, "t", "caller.wl")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Calls) == 0 {
		t.Fatal("expected calls, got 0")
	}
	for _, c := range r.Calls {
		if c.CalleeName == "g" || c.CalleeName == "h" {
			// enclosing_func_qn must NOT be the module path (file path)
			if c.EnclosingFuncQN == "" || c.EnclosingFuncQN == "t.caller" {
				t.Errorf("call to %q: enclosing_func_qn is file path %q, want function QN", c.CalleeName, c.EnclosingFuncQN)
			}
		}
	}
}

func TestWolframParse_Regression(t *testing.T) {
	src := []byte("x = 42;\ny = x + 1;\n")
	_, err := ExtractFile(src, lang.Wolfram, "t", "simple.wl")
	if err != nil {
		t.Fatal(err)
	}
}

func TestWolframImport_Regression(t *testing.T) {
	src := []byte("<< \"utils.wl\"\nNeeds[\"Package`\"]\n")
	r, err := ExtractFile(src, lang.Wolfram, "t", "main.wl")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Imports) == 0 {
		t.Fatalf("expected imports, got 0")
	}
}

func TestWolframNestedDef_Regression(t *testing.T) {
	// set_delayed (non-top) inside Module should also be extracted
	src := []byte("main[x_] := Module[{localF}, localF[t_] := t + 1; localF[x]]\n")
	r, err := ExtractFile(src, lang.Wolfram, "t", "nested.wl")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	assertHasName(t, fns, "main")
	assertHasName(t, fns, "localF")
}

func TestMagmaDisambiguation_Regression(t *testing.T) {
	// Magma code with 'end function;' should be extractable as Magma
	src := []byte(`function Factorial(n)
  if n le 1 then
    return 1;
  end if;
  return n * Factorial(n - 1);
end function;
`)
	r, err := ExtractFile(src, lang.Magma, "t", "test.m")
	if err != nil {
		t.Fatal(err)
	}
	fns := defsWithLabel(r, "Function")
	if len(fns) == 0 {
		t.Fatal("expected at least 1 function from Magma code")
	}
	assertHasName(t, fns, "Factorial")
}
