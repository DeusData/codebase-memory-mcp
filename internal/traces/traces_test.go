package traces

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func TestExtractServiceName(t *testing.T) {
	r := Resource{
		Attributes: []Attribute{
			{Key: "service.name", Value: AttributeValue{StringValue: "order-service"}},
		},
	}
	if got := extractServiceName(r); got != "order-service" {
		t.Errorf("expected order-service, got %s", got)
	}
}

func TestExtractHTTPInfo(t *testing.T) {
	span := Span{
		Kind: 2,
		Attributes: []Attribute{
			{Key: "http.method", Value: AttributeValue{StringValue: "GET"}},
			{Key: "http.route", Value: AttributeValue{StringValue: "/api/orders"}},
			{Key: "http.status_code", Value: AttributeValue{StringValue: "200"}},
		},
		StartTime: "1000000000",
		EndTime:   "1050000000",
	}
	info := extractHTTPInfo(&span, "svc")
	if info == nil {
		t.Fatal("expected HTTPSpanInfo")
	}
	if info.Method != "GET" {
		t.Errorf("expected GET, got %s", info.Method)
	}
	if info.Path != "/api/orders" {
		t.Errorf("expected /api/orders, got %s", info.Path)
	}
	if info.DurationNs != 50000000 {
		t.Errorf("expected 50000000ns, got %d", info.DurationNs)
	}
}

func TestExtractHTTPInfoNonHTTPSpan(t *testing.T) {
	span := Span{
		Kind: 1,
		Attributes: []Attribute{
			{Key: "db.system", Value: AttributeValue{StringValue: "postgresql"}},
		},
	}
	info := extractHTTPInfo(&span, "svc")
	if info != nil {
		t.Error("expected nil for non-HTTP span")
	}
}

func TestExtractPathFromURL(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://example.com/api/orders", "/api/orders"},
		{"http://localhost:8080/health?check=true", "/health"},
		{"not-a-url", ""},
	}
	for _, tt := range tests {
		if got := extractPathFromURL(tt.url); got != tt.want {
			t.Errorf("extractPathFromURL(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
}

func TestIngestOTLPJSON(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "test-proj"
	_ = s.UpsertProject(project, "/tmp/test")

	srcID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "caller",
		QualifiedName: "svcA.caller",
	})
	tgtID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "handler",
		QualifiedName: "svcB.handler",
	})
	_, _ = s.InsertEdge(&store.Edge{
		Project: project, SourceID: srcID, TargetID: tgtID,
		Type: "HTTP_CALLS",
		Properties: map[string]any{
			"url_path":        "/api/orders",
			"confidence":      0.5,
			"confidence_band": "medium",
		},
	})

	fixture := `{
		"resourceSpans": [{
			"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "order-service"}}]},
			"scopeSpans": [{
				"spans": [{
					"traceId": "abc123",
					"spanId": "def456",
					"name": "GET /api/orders",
					"kind": 2,
					"startTimeUnixNano": "1000000000",
					"endTimeUnixNano": "1050000000",
					"attributes": [
						{"key": "http.method", "value": {"stringValue": "GET"}},
						{"key": "http.route", "value": {"stringValue": "/api/orders"}}
					],
					"status": {"code": 1}
				}]
			}]
		}]
	}`

	tmpFile := filepath.Join(t.TempDir(), "traces.json")
	if err := os.WriteFile(tmpFile, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := Ingest(s, project, tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	if result.SpansProcessed != 1 {
		t.Errorf("expected 1 span, got %d", result.SpansProcessed)
	}
	if result.EdgesValidated != 1 {
		t.Errorf("expected 1 validated, got %d", result.EdgesValidated)
	}

	// Check edge was enriched
	edges, _ := s.FindEdgesByType(project, "HTTP_CALLS")
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	e := edges[0]
	if v, ok := e.Properties["validated_by_trace"].(bool); !ok || !v {
		t.Error("expected validated_by_trace=true")
	}
	if conf, ok := e.Properties["confidence"].(float64); !ok || conf < 0.6 {
		t.Errorf("expected boosted confidence >= 0.6, got %v", e.Properties["confidence"])
	}
}

func TestCalculateP99(t *testing.T) {
	values := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	p99 := calculateP99(values)
	if p99 != 100 {
		t.Errorf("expected 100, got %d", p99)
	}

	single := []int64{42}
	if got := calculateP99(single); got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}
