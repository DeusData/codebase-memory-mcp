package pipeline

import "testing"

func TestFuzzyResolve_SingleCandidate(t *testing.T) {
	reg := NewFunctionRegistry()
	reg.Register("CreateOrder", "svcA.handlers.CreateOrder", "Function")
	reg.Register("ValidateOrder", "svcB.validators.ValidateOrder", "Function")

	// Normal resolve with no import map should find unique name
	result := reg.Resolve("CreateOrder", "svcC.caller", nil)
	if result != "svcA.handlers.CreateOrder" {
		t.Errorf("Resolve: expected svcA.handlers.CreateOrder, got %s", result)
	}

	// FuzzyResolve should find by simple name even with unknown prefix
	fuzzyResult, ok := reg.FuzzyResolve("unknownPkg.CreateOrder", "svcC.caller")
	if !ok {
		t.Fatal("expected fuzzy match")
	}
	if fuzzyResult != "svcA.handlers.CreateOrder" {
		t.Errorf("expected svcA.handlers.CreateOrder, got %s", fuzzyResult)
	}
}

func TestFuzzyResolve_NonExistentName(t *testing.T) {
	reg := NewFunctionRegistry()
	reg.Register("CreateOrder", "svcA.handlers.CreateOrder", "Function")

	_, ok := reg.FuzzyResolve("NonExistent", "svcC.caller")
	if ok {
		t.Fatal("expected no fuzzy match for non-existent name")
	}
}

func TestFuzzyResolve_MultipleCandidates_BestByDistance(t *testing.T) {
	reg := NewFunctionRegistry()
	reg.Register("Process", "svcA.handlers.Process", "Function")
	reg.Register("Process", "svcB.handlers.Process", "Function")

	// Caller is in svcA — should prefer svcA.handlers.Process
	fuzzyResult, ok := reg.FuzzyResolve("unknown.Process", "svcA.other")
	if !ok {
		t.Fatal("expected fuzzy match")
	}
	if fuzzyResult != "svcA.handlers.Process" {
		t.Errorf("expected svcA.handlers.Process, got %s", fuzzyResult)
	}

	// Caller is in svcB — should prefer svcB.handlers.Process
	fuzzyResult, ok = reg.FuzzyResolve("unknown.Process", "svcB.other")
	if !ok {
		t.Fatal("expected fuzzy match")
	}
	if fuzzyResult != "svcB.handlers.Process" {
		t.Errorf("expected svcB.handlers.Process, got %s", fuzzyResult)
	}
}

func TestFuzzyResolve_SimpleNameExtraction(t *testing.T) {
	reg := NewFunctionRegistry()
	reg.Register("DoWork", "myproject.utils.DoWork", "Function")

	// Deeply qualified callee — should extract "DoWork" as simple name
	fuzzyResult, ok := reg.FuzzyResolve("some.deep.module.DoWork", "myproject.caller")
	if !ok {
		t.Fatal("expected fuzzy match")
	}
	if fuzzyResult != "myproject.utils.DoWork" {
		t.Errorf("expected myproject.utils.DoWork, got %s", fuzzyResult)
	}
}

func TestFuzzyResolve_NoMatchForBareName(t *testing.T) {
	reg := NewFunctionRegistry()
	// Register nothing

	_, ok := reg.FuzzyResolve("SomeFunc", "myproject.caller")
	if ok {
		t.Fatal("expected no fuzzy match on empty registry")
	}
}

func TestRegistryExists(t *testing.T) {
	reg := NewFunctionRegistry()
	reg.Register("Foo", "pkg.module.Foo", "Function")
	reg.Register("Bar", "pkg.module.Bar", "Method")

	if !reg.Exists("pkg.module.Foo") {
		t.Error("expected Foo to exist")
	}
	if !reg.Exists("pkg.module.Bar") {
		t.Error("expected Bar to exist")
	}
	if reg.Exists("pkg.module.Missing") {
		t.Error("expected Missing to not exist")
	}
	if reg.Exists("") {
		t.Error("expected empty string to not exist")
	}
}
