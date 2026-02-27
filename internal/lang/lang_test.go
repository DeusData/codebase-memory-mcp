package lang

import "testing"

func TestForExtension(t *testing.T) {
	tests := []struct {
		ext  string
		lang Language
	}{
		{".py", Python},
		{".go", Go},
		{".js", JavaScript},
		{".ts", TypeScript},
		{".tsx", TSX},
		{".rs", Rust},
		{".java", Java},
		{".cpp", CPP},
		{".h", CPP},
		{".cs", CSharp},
		{".php", PHP},
		{".lua", Lua},
		{".scala", Scala},
		{".kt", Kotlin},
		{".kts", Kotlin},
	}
	for _, tt := range tests {
		spec := ForExtension(tt.ext)
		if spec == nil {
			t.Errorf("ForExtension(%q) = nil, want %s", tt.ext, tt.lang)
			continue
		}
		if spec.Language != tt.lang {
			t.Errorf("ForExtension(%q).Language = %s, want %s", tt.ext, spec.Language, tt.lang)
		}
	}
}

func TestForLanguage(t *testing.T) {
	for _, lang := range AllLanguages() {
		spec := ForLanguage(lang)
		if spec == nil {
			t.Errorf("ForLanguage(%s) = nil", lang)
		}
	}
}

func TestUnknownExtension(t *testing.T) {
	if spec := ForExtension(".xyz"); spec != nil {
		t.Errorf("ForExtension(.xyz) should be nil, got %v", spec)
	}
}

func TestGoSpec(t *testing.T) {
	spec := ForLanguage(Go)
	if spec == nil {
		t.Fatal("Go spec not registered")
	}
	if len(spec.FunctionNodeTypes) != 2 {
		t.Errorf("Go FunctionNodeTypes: got %d, want 2", len(spec.FunctionNodeTypes))
	}
	// Should contain function_declaration and method_declaration
	found := map[string]bool{}
	for _, nt := range spec.FunctionNodeTypes {
		found[nt] = true
	}
	if !found["function_declaration"] || !found["method_declaration"] {
		t.Errorf("Go FunctionNodeTypes missing expected types: %v", spec.FunctionNodeTypes)
	}
}

func TestPythonSpec(t *testing.T) {
	spec := ForLanguage(Python)
	if spec == nil {
		t.Fatal("Python spec not registered")
	}
	if spec.PackageIndicators[0] != "__init__.py" {
		t.Errorf("Python PackageIndicators: got %v, want [__init__.py]", spec.PackageIndicators)
	}
}