package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func TestTokenizeDecorator(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"@Override", []string{"override"}},
		{"@Deprecated", []string{"deprecated"}},
		{"@Test", []string{"test"}},
		{"@login_required", []string{"login", "required"}},
		{"@cache", []string{"cache"}},
		{"@pytest.fixture", []string{"pytest", "fixture"}},
		{`@GetMapping("/api")`, []string{"mapping"}}, // "get" is stopword
		{`@PostMapping("/api")`, []string{"post", "mapping"}},
		{`@Transactional`, []string{"transactional"}},
		{`@MessageMapping("/chat")`, []string{"message", "mapping"}},
		{`#[test]`, []string{"test"}},
		{`#[derive(Debug)]`, []string{"derive"}},
		{"@app.get(\"/api\")", nil},                  // both "app" and "get" are stopwords
		{"@router.post(\"/api\")", []string{"post"}}, // "router" is stopword, "post" passes
		{"@x", nil}, // too short after filtering
		{"", nil},   // empty
		{"@click.command", []string{"click", "command"}},
		{"@celery.task", []string{"celery", "task"}},
	}
	for _, tt := range tests {
		got := tokenizeDecorator(tt.input)
		if !sliceEqual(got, tt.want) {
			t.Errorf("tokenizeDecorator(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestSplitCamelCase(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"GetMapping", []string{"Get", "Mapping"}},
		{"getMessage", []string{"get", "Message"}},
		{"cache", []string{"cache"}},
		{"HTMLParser", []string{"HTMLParser"}}, // no lowercase→uppercase transition
		{"", nil},
	}
	for _, tt := range tests {
		got := splitCamelCase(tt.input)
		if !sliceEqual(got, tt.want) {
			t.Errorf("splitCamelCase(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestDecoratorTagAutoDiscovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-dectag-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create Python files with repeated decorators
	writeFile(t, filepath.Join(dir, "views.py"), `
from functools import cache

@login_required
def list_orders():
    pass

@login_required
def get_order():
    pass

@cache
def compute_total():
    pass

@cache
def compute_tax():
    pass

@unique_helper
def special():
    pass
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

	// Check decorator_tags on nodes
	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")

	tagMap := map[string][]string{} // funcName → tags
	for _, f := range funcs {
		tags, ok := f.Properties["decorator_tags"]
		if !ok {
			continue
		}
		tagList, ok := tags.([]any)
		if !ok {
			continue
		}
		var ts []string
		for _, tag := range tagList {
			if s, ok := tag.(string); ok {
				ts = append(ts, s)
			}
		}
		tagMap[f.Name] = ts
	}

	// "login" and "required" appear on 2 nodes → should be tags
	assertHasTag(t, tagMap, "list_orders", "login")
	assertHasTag(t, tagMap, "list_orders", "required")
	assertHasTag(t, tagMap, "get_order", "login")
	assertHasTag(t, tagMap, "get_order", "required")

	// "cache" appears on 2 nodes → should be a tag
	assertHasTag(t, tagMap, "compute_total", "cache")
	assertHasTag(t, tagMap, "compute_tax", "cache")

	// "unique" and "helper" appear on only 1 node → should NOT be tags
	if tags, ok := tagMap["special"]; ok {
		for _, tag := range tags {
			if tag == "unique" || tag == "helper" {
				t.Errorf("special should not have tag %q (freq < 2)", tag)
			}
		}
	}
}

func TestDecoratorTagJavaClassMethods(t *testing.T) {
	dir, err := os.MkdirTemp("", "cgm-javadec-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeFile(t, filepath.Join(dir, "Controller.java"), `
class OwnerController {
    @GetMapping("/owners")
    public void listOwners() {}

    @GetMapping("/owners/{id}")
    public void showOwner() {}

    @PostMapping("/owners")
    public void createOwner() {}

    @Transactional
    @PostMapping("/owners/{id}")
    public void updateOwner() {}
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

	// Verify decorators are extracted on class methods
	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")

	decoMap := map[string][]string{}
	tagMap := map[string][]string{}
	for _, m := range methods {
		if ds := extractStringSliceProp(m.Properties, "decorators"); len(ds) > 0 {
			decoMap[m.Name] = ds
		}
		if ts := extractStringSliceProp(m.Properties, "decorator_tags"); len(ts) > 0 {
			tagMap[m.Name] = ts
		}
	}

	// decorators should be extracted
	if len(decoMap["listOwners"]) == 0 {
		t.Error("listOwners should have decorators")
	}
	if len(decoMap["showOwner"]) == 0 {
		t.Error("showOwner should have decorators")
	}

	// "mapping" appears on all 4 methods → should be a tag
	assertHasTag(t, tagMap, "listOwners", "mapping")
	assertHasTag(t, tagMap, "showOwner", "mapping")

	// "post" appears on 2 methods → should be a tag
	assertHasTag(t, tagMap, "createOwner", "post")
	assertHasTag(t, tagMap, "updateOwner", "post")
}

func assertHasTag(t *testing.T, tagMap map[string][]string, funcName, tag string) {
	t.Helper()
	tags, ok := tagMap[funcName]
	if !ok {
		t.Errorf("%s has no decorator_tags", funcName)
		return
	}
	for _, tt := range tags {
		if tt == tag {
			return
		}
	}
	t.Errorf("%s missing tag %q, got %v", funcName, tag, tags)
}

// extractStringSliceProp extracts a []any property as []string.
func extractStringSliceProp(props map[string]any, key string) []string {
	val, ok := props[key]
	if !ok {
		return nil
	}
	list, ok := val.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func sliceEqual(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
