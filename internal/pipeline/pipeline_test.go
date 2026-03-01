package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func setupTestRepo(t *testing.T) (dir string, cleanup func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "cgm-test-*")
	if err != nil {
		t.Fatal(err)
	}
	cleanup = func() { os.RemoveAll(dir) }

	// Create a simple Go project
	writeFile(t, filepath.Join(dir, "main.go"), `package main

func main() {
	result := Add(1, 2)
	_ = result
}

func Add(a, b int) int {
	return a + b
}
`)

	writeFile(t, filepath.Join(dir, "service", "service.go"), `package service

func ProcessOrder(id string) error {
	return nil
}

func SubmitOrder(order interface{}) error {
	ProcessOrder("test")
	return nil
}
`)

	return dir, cleanup
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineRun(t *testing.T) {
	repoDir, cleanup := setupTestRepo(t)
	defer cleanup()

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, repoDir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Check node counts
	nodeCount, _ := s.CountNodes(p.ProjectName)
	if nodeCount == 0 {
		t.Fatal("expected nodes, got 0")
	}
	t.Logf("Total nodes: %d", nodeCount)

	// Check that Functions were found
	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	t.Logf("Functions: %d", len(funcs))
	for _, f := range funcs {
		t.Logf("  %s (qn=%s, sig=%v)", f.Name, f.QualifiedName, f.Properties["signature"])
	}
	if len(funcs) < 3 { // main, Add, at minimum
		t.Errorf("expected at least 3 functions, got %d", len(funcs))
	}

	// Check that ProcessOrder exists
	found, _ := s.FindNodesByName(p.ProjectName, "ProcessOrder")
	if len(found) == 0 {
		t.Error("ProcessOrder not found")
	}

	// Check that Module nodes exist
	modules, _ := s.FindNodesByLabel(p.ProjectName, "Module")
	if len(modules) < 2 {
		t.Errorf("expected at least 2 modules, got %d", len(modules))
	}

	// Check edges exist
	edgeCount, _ := s.CountEdges(p.ProjectName)
	t.Logf("Total edges: %d", edgeCount)
	if edgeCount == 0 {
		t.Error("expected edges, got 0")
	}

	// Check CALLS edges
	// SubmitOrder calls ProcessOrder
	sender, _ := s.FindNodesByName(p.ProjectName, "SubmitOrder")
	if len(sender) > 0 {
		edges, _ := s.FindEdgesBySourceAndType(sender[0].ID, "CALLS")
		t.Logf("SubmitOrder CALLS edges: %d", len(edges))
	}
}

func TestPipelinePythonProject(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-py-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "main.py"), `
def greet(name):
    return f"Hello, {name}"

def process():
    result = greet("world")
    return result
`)

	writeFile(t, filepath.Join(dir, "utils.py"), `
API_URL = "https://example.com/api"
MAX_RETRIES = 3

def fetch_data(url):
    pass

class DataProcessor:
    def transform(self, data):
        return data
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	t.Logf("Python functions: %d", len(funcs))
	if len(funcs) < 3 { // greet, process, fetch_data
		t.Errorf("expected at least 3 functions, got %d", len(funcs))
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	if len(classes) < 1 {
		t.Errorf("expected at least 1 class, got %d", len(classes))
	}

	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	if len(methods) < 1 {
		t.Errorf("expected at least 1 method, got %d", len(methods))
	}
}

// TestGoCrossPackageCallViaImport verifies that a Go function in package A
// that imports package B and calls B.Func() gets a CALLS edge resolved.
func TestGoCrossPackageCallViaImport(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-go-import-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Package "svc" defines ProcessOrder
	writeFile(t, filepath.Join(dir, "svc", "handler.go"), `package svc

func ProcessOrder(id string) error {
	return nil
}
`)

	// Package "main" imports "svc" and calls svc.ProcessOrder
	writeFile(t, filepath.Join(dir, "main.go"), `package main

import "example.com/myapp/svc"

func run() {
	svc.ProcessOrder("123")
}
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Verify ProcessOrder exists
	targets, _ := s.FindNodesByName(p.ProjectName, "ProcessOrder")
	if len(targets) == 0 {
		t.Fatal("ProcessOrder not found in store")
	}
	t.Logf("ProcessOrder QN: %s", targets[0].QualifiedName)

	// Verify run() exists
	callers, _ := s.FindNodesByName(p.ProjectName, "run")
	if len(callers) == 0 {
		t.Fatal("run() not found in store")
	}

	// Check that run() has a CALLS edge to ProcessOrder
	edges, _ := s.FindEdgesBySourceAndType(callers[0].ID, "CALLS")
	found := false
	for _, e := range edges {
		if e.TargetID == targets[0].ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected CALLS edge from run() to ProcessOrder via import, but none found")
		t.Logf("run() CALLS edges: %d", len(edges))
		for _, e := range edges {
			t.Logf("  target_id=%d", e.TargetID)
		}
	}
}

// TestPythonCrossModuleCallViaImport verifies that a Python file that does
// "from utils import fetch_data" and calls fetch_data() gets a CALLS edge.
func TestPythonCrossModuleCallViaImport(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-py-import-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "utils.py"), `
def fetch_data(url):
    return {"status": "ok"}
`)

	writeFile(t, filepath.Join(dir, "main.py"), `
from utils import fetch_data

def process():
    result = fetch_data("https://example.com")
    return result
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Verify fetch_data exists
	targets, _ := s.FindNodesByName(p.ProjectName, "fetch_data")
	if len(targets) == 0 {
		t.Fatal("fetch_data not found in store")
	}
	t.Logf("fetch_data QN: %s", targets[0].QualifiedName)

	// Verify process() exists
	callers, _ := s.FindNodesByName(p.ProjectName, "process")
	if len(callers) == 0 {
		t.Fatal("process() not found in store")
	}

	// Check that process() has a CALLS edge to fetch_data
	edges, _ := s.FindEdgesBySourceAndType(callers[0].ID, "CALLS")
	found := false
	for _, e := range edges {
		if e.TargetID == targets[0].ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected CALLS edge from process() to fetch_data via import, but none found")
		t.Logf("process() CALLS edges: %d", len(edges))
	}
}

// TestPythonMethodDispatchViaTypeInference verifies that type inference allows
// resolving method calls: p = DataProcessor() then p.transform() -> CALLS DataProcessor.transform
func TestPythonMethodDispatchViaTypeInference(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-py-type-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "processor.py"), `
class DataProcessor:
    def transform(self, data):
        return data.upper()

    def validate(self, data):
        return len(data) > 0
`)

	writeFile(t, filepath.Join(dir, "main.py"), `
from processor import DataProcessor

def run():
    p = DataProcessor()
    result = p.transform("hello")
    return result
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Verify DataProcessor.transform exists as a Method
	methods, _ := s.FindNodesByName(p.ProjectName, "transform")
	if len(methods) == 0 {
		t.Fatal("transform method not found in store")
	}
	t.Logf("transform QN: %s", methods[0].QualifiedName)

	// Verify run() exists
	callers, _ := s.FindNodesByName(p.ProjectName, "run")
	if len(callers) == 0 {
		t.Fatal("run() not found in store")
	}

	// Check that run() has a CALLS edge to DataProcessor.transform
	edges, _ := s.FindEdgesBySourceAndType(callers[0].ID, "CALLS")
	found := false
	for _, e := range edges {
		if e.TargetID == methods[0].ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected CALLS edge from run() to DataProcessor.transform via type inference, but none found")
		t.Logf("run() CALLS edges: %d", len(edges))
		for _, e := range edges {
			t.Logf("  target_id=%d", e.TargetID)
		}
	}
}

// TestGoTypeClassification verifies that Go interfaces, structs, and type aliases
// are correctly classified as Interface, Class, and Type respectively.
func TestGoTypeClassification(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-go-types-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "types.go"), `package types

type Reader interface {
	Read(p []byte) (n int, err error)
}

type Writer interface {
	Write(p []byte) (n int, err error)
}

type Config struct {
	Host string
	Port int
}

type ID = string
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Check Interface nodes (Reader, Writer)
	interfaces, _ := s.FindNodesByLabel(p.ProjectName, "Interface")
	if len(interfaces) != 2 {
		t.Errorf("expected 2 Interface nodes, got %d", len(interfaces))
		for _, n := range interfaces {
			t.Logf("  Interface: %s", n.Name)
		}
	}

	// Check Class nodes (Config struct)
	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	if len(classes) != 1 {
		t.Errorf("expected 1 Class node (struct), got %d", len(classes))
		for _, n := range classes {
			t.Logf("  Class: %s", n.Name)
		}
	} else if classes[0].Name != "Config" {
		t.Errorf("expected Class name 'Config', got %q", classes[0].Name)
	}

	// Check Type nodes (ID type alias)
	types, _ := s.FindNodesByLabel(p.ProjectName, "Type")
	if len(types) != 1 {
		t.Errorf("expected 1 Type node (alias), got %d", len(types))
		for _, n := range types {
			t.Logf("  Type: %s", n.Name)
		}
	} else if types[0].Name != "ID" {
		t.Errorf("expected Type name 'ID', got %q", types[0].Name)
	}
}

// TestGoGroupedTypeDeclaration verifies that grouped type declarations
// (type ( ... )) also work correctly.
func TestGoGroupedTypeDeclaration(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-go-grouped-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "models.go"), `package models

type (
	Request struct {
		URL string
	}

	Response struct {
		Status int
	}

	Handler interface {
		Handle(req Request) Response
	}
)
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Should find 2 structs (Class) and 1 interface
	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	if len(classes) != 2 {
		t.Errorf("expected 2 Class nodes (structs), got %d", len(classes))
	}

	interfaces, _ := s.FindNodesByLabel(p.ProjectName, "Interface")
	if len(interfaces) != 1 {
		t.Errorf("expected 1 Interface node, got %d", len(interfaces))
	}
}

// TestPipelineKotlinProject verifies that Kotlin files are parsed and indexed
// correctly, producing Function, Class, and Method nodes with CALLS edges.
func TestPipelineKotlinProject(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-kt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "Main.kt"), `
fun greet(name: String): String {
    return "Hello, $name"
}

fun main() {
    val result = greet("world")
    println(result)
}
`)

	writeFile(t, filepath.Join(dir, "Service.kt"), `
class OrderService {
    fun processOrder(id: String): Boolean {
        return true
    }

    fun submitOrder(order: String): Boolean {
        return processOrder(order)
    }
}

object Config {
    val API_URL = "https://example.com/api"
}
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	t.Logf("Kotlin functions: %d", len(funcs))
	if len(funcs) < 2 { // greet, main
		t.Errorf("expected at least 2 functions, got %d", len(funcs))
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	t.Logf("Kotlin classes: %d", len(classes))
	// OrderService + Config (object_declaration)
	if len(classes) < 1 {
		t.Errorf("expected at least 1 class, got %d", len(classes))
	}

	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	t.Logf("Kotlin methods: %d", len(methods))
	if len(methods) < 2 { // processOrder, submitOrder
		t.Errorf("expected at least 2 methods, got %d", len(methods))
	}

	// Check that greet exists
	found, _ := s.FindNodesByName(p.ProjectName, "greet")
	if len(found) == 0 {
		t.Error("greet not found")
	}

	// Check modules exist
	modules, _ := s.FindNodesByLabel(p.ProjectName, "Module")
	if len(modules) < 2 {
		t.Errorf("expected at least 2 modules, got %d", len(modules))
	}

	// Check edges exist
	edgeCount, _ := s.CountEdges(p.ProjectName)
	t.Logf("Total edges: %d", edgeCount)
	if edgeCount == 0 {
		t.Error("expected edges, got 0")
	}
}

// TestLuaAnonymousFunctionExtraction verifies that MoonScript-style Lua code
// (local f = function(...) end) extracts Function nodes correctly.
func TestLuaAnonymousFunctionExtraction(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-lua-anon-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "app.lua"), `local run_before_filter
run_before_filter = function(filter, r)
  return filter(r)
end

local validate = function(data)
  return data ~= nil
end

function named_func(x)
  return x + 1
end
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	t.Logf("Lua functions: %d", len(funcs))
	for _, f := range funcs {
		t.Logf("  %s (qn=%s)", f.Name, f.QualifiedName)
	}

	// Expect: run_before_filter, validate, named_func (3 functions)
	if len(funcs) < 3 {
		t.Errorf("expected at least 3 functions (incl. anonymous assigned), got %d", len(funcs))
	}

	// Verify specific functions exist
	for _, name := range []string{"run_before_filter", "validate", "named_func"} {
		found, _ := s.FindNodesByName(p.ProjectName, name)
		if len(found) == 0 {
			t.Errorf("function %q not found", name)
		}
	}
}

// TestCSharpModernFeatures verifies that modern C# files (file-scoped namespaces,
// primary constructors, expression-bodied members) are fully extracted.
func TestCSharpModernFeatures(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-csharp-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "Controller.cs"), `namespace Conduit.Features;

public class UsersController {
	public void Get() {}
	public void Create(string name) {}
}
`)

	writeFile(t, filepath.Join(dir, "Model.cs"), `namespace Conduit.Models {
	class User {
		public string Name { get; set; }
		public int GetAge() { return 0; }
	}
}
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	modules, _ := s.FindNodesByLabel(p.ProjectName, "Module")
	t.Logf("C# modules: %d", len(modules))
	if len(modules) < 2 {
		t.Errorf("expected at least 2 Module nodes for .cs files, got %d", len(modules))
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	t.Logf("C# classes: %d", len(classes))
	if len(classes) < 2 {
		t.Errorf("expected at least 2 Class nodes, got %d", len(classes))
	}

	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	t.Logf("C# methods: %d", len(methods))
	if len(methods) < 3 { // Get, Create, GetAge
		t.Errorf("expected at least 3 Method nodes, got %d", len(methods))
	}
}

// TestBOMStripping verifies that files with UTF-8 BOM are parsed correctly.
func TestBOMStripping(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-bom-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write a Go file WITH UTF-8 BOM prefix
	bom := []byte{0xEF, 0xBB, 0xBF}
	src := []byte("package main\n\nfunc BOMFunc() {}\n")
	content := make([]byte, 0, len(bom)+len(src))
	content = append(content, bom...)
	content = append(content, src...)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bom.go"), content, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	found, _ := s.FindNodesByName(p.ProjectName, "BOMFunc")
	if len(found) == 0 {
		t.Error("BOMFunc not found — BOM stripping may have failed")
	}
}

// TestFunctionRegistry tests the registry in isolation.
func TestFunctionRegistry(t *testing.T) {
	r := NewFunctionRegistry()

	r.Register("Foo", "proj.pkg.Foo", "Function")
	r.Register("Bar", "proj.pkg.Bar", "Function")
	r.Register("Foo", "proj.other.Foo", "Function")
	r.Register("transform", "proj.utils.DataProcessor.transform", "Method")

	// FindByName returns all entries
	foos := r.FindByName("Foo")
	if len(foos) != 2 {
		t.Errorf("expected 2 Foo entries, got %d", len(foos))
	}

	// FindEndingWith
	matches := r.FindEndingWith("DataProcessor.transform")
	if len(matches) != 1 {
		t.Errorf("expected 1 match for DataProcessor.transform, got %d", len(matches))
	}

	// Resolve same-module
	qn := r.Resolve("Foo", "proj.pkg", nil)
	if qn != "proj.pkg.Foo" {
		t.Errorf("expected proj.pkg.Foo, got %s", qn)
	}

	// Resolve via import map
	imports := map[string]string{"other": "proj.other"}
	qn = r.Resolve("other.Foo", "proj.pkg", imports)
	if qn != "proj.other.Foo" {
		t.Errorf("expected proj.other.Foo, got %s", qn)
	}

	// Resolve unique name
	qn = r.Resolve("Bar", "proj.unrelated", nil)
	if qn != "proj.pkg.Bar" {
		t.Errorf("expected proj.pkg.Bar, got %s", qn)
	}
}

// TestPipelineRunCancellation verifies that a pre-cancelled context makes Run() return context.Canceled.
func TestPipelineRunCancellation(t *testing.T) {
	repoDir, cleanup := setupTestRepo(t)
	defer cleanup()

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	p := New(ctx, s, repoDir)
	err = p.Run()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestProjectNameFromPath verifies unique project names from absolute paths.
func TestProjectNameFromPath(t *testing.T) {
	tests := []struct{ path, want string }{
		{"/tmp/bench/erlang/lib/stdlib/src", "tmp-bench-erlang-lib-stdlib-src"},
		{"/Users/martin/projects/myapp", "Users-martin-projects-myapp"},
		{"/home/user/repo", "home-user-repo"},
		{"/single", "single"},
	}
	for _, tt := range tests {
		got := ProjectNameFromPath(tt.path)
		if got != tt.want {
			t.Errorf("ProjectNameFromPath(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

// TestProjectNameUniqueness verifies two paths with same base name produce different project names.
func TestProjectNameUniqueness(t *testing.T) {
	a := ProjectNameFromPath("/tmp/bench/zig/lib/std")
	b := ProjectNameFromPath("/tmp/bench/erlang/lib/stdlib/src")
	if a == b {
		t.Errorf("collision: %q == %q", a, b)
	}
}
