package orgdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParsePackageJSON_ExtractsGHLDeps(t *testing.T) {
	dir := t.TempDir()
	pkgJSON := `{
		"name": "ghl-revex-backend",
		"dependencies": {
			"@platform-core/base-service": "^3.2.0",
			"@platform-core/pubsub": "^1.0.0",
			"express": "^4.18.0",
			"@gohighlevel/ghl-ui": "^2.0.0"
		},
		"devDependencies": {
			"@platform-core/eslint-config-ghl": "^1.0.0",
			"jest": "^29.0.0"
		}
	}`
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(pkgJSON), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	deps, err := ParsePackageJSON(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatalf("ParsePackageJSON: %v", err)
	}

	if len(deps) != 4 {
		t.Fatalf("deps count: got %d, want 4 (base-service, pubsub, eslint-config-ghl, ghl-ui); got: %v", len(deps), deps)
	}

	found := map[string]bool{}
	for _, d := range deps {
		key := d.Scope + "/" + d.Name
		found[key] = true
		if key == "@platform-core/base-service" {
			if d.DepType != "dependencies" {
				t.Errorf("base-service dep_type: got %q, want %q", d.DepType, "dependencies")
			}
			if d.VersionSpec != "^3.2.0" {
				t.Errorf("base-service version: got %q, want %q", d.VersionSpec, "^3.2.0")
			}
		}
	}
	if !found["@platform-core/base-service"] {
		t.Error("missing @platform-core/base-service")
	}
	if !found["@platform-core/pubsub"] {
		t.Error("missing @platform-core/pubsub")
	}
	if !found["@gohighlevel/ghl-ui"] {
		t.Error("missing @gohighlevel/ghl-ui")
	}
}

// ---------- ParsePackageName ----------

func TestParsePackageName_InternalScope(t *testing.T) {
	dir := t.TempDir()
	pkgJSON := `{"name": "@platform-core/base-service", "version": "3.2.0"}`
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(pkgJSON), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	scope, name, err := ParsePackageName(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatalf("ParsePackageName: %v", err)
	}
	if scope != "@platform-core" || name != "base-service" {
		t.Errorf("got (%q, %q), want (@platform-core, base-service)", scope, name)
	}
}

func TestParsePackageName_ExternalScope(t *testing.T) {
	dir := t.TempDir()
	pkgJSON := `{"name": "@nestjs/common"}`
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(pkgJSON), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	scope, name, err := ParsePackageName(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatalf("ParsePackageName: %v", err)
	}
	if scope != "" || name != "" {
		t.Errorf("expected empty for external scope, got (%q, %q)", scope, name)
	}
}

func TestParsePackageName_UnscopedName(t *testing.T) {
	dir := t.TempDir()
	pkgJSON := `{"name": "simple-app"}`
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(pkgJSON), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	scope, name, err := ParsePackageName(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatalf("ParsePackageName: %v", err)
	}
	if scope != "" || name != "" {
		t.Errorf("expected empty for unscoped name, got (%q, %q)", scope, name)
	}
}

func TestParsePackageJSON_MissingFile(t *testing.T) {
	_, err := ParsePackageJSON("/nonexistent/package.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestParsePackageJSON_NoRelevantDeps(t *testing.T) {
	dir := t.TempDir()
	pkgJSON := `{"name": "simple-app", "dependencies": {"express": "^4.18.0"}}`
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(pkgJSON), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	deps, err := ParsePackageJSON(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatalf("ParsePackageJSON: %v", err)
	}
	if len(deps) != 0 {
		t.Errorf("deps count: got %d, want 0", len(deps))
	}
}

func TestParsePackageJSON_IncludesDevDeps(t *testing.T) {
	dir := t.TempDir()
	pkgJSON := `{
		"name": "test",
		"devDependencies": {
			"@platform-core/eslint-config-ghl": "^1.0.0"
		}
	}`
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(pkgJSON), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	deps, err := ParsePackageJSON(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatalf("ParsePackageJSON: %v", err)
	}
	if len(deps) != 1 {
		t.Fatalf("deps count: got %d, want 1", len(deps))
	}
	if deps[0].DepType != "devDependencies" {
		t.Errorf("dep_type: got %q, want %q", deps[0].DepType, "devDependencies")
	}
}
