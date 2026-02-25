package traces

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// OTLPExport represents the top-level structure of an OTLP JSON export.
type OTLPExport struct {
	ResourceSpans []ResourceSpan `json:"resourceSpans"`
}

// ResourceSpan contains spans from a single service/resource.
type ResourceSpan struct {
	Resource   Resource    `json:"resource"`
	ScopeSpans []ScopeSpan `json:"scopeSpans"`
}

// Resource describes the service that produced the spans.
type Resource struct {
	Attributes []Attribute `json:"attributes"`
}

// ScopeSpan groups spans by instrumentation scope.
type ScopeSpan struct {
	Spans []Span `json:"spans"`
}

// Span represents a single trace span.
type Span struct {
	TraceID      string      `json:"traceId"`
	SpanID       string      `json:"spanId"`
	ParentSpanID string      `json:"parentSpanId"`
	Name         string      `json:"name"`
	Kind         int         `json:"kind"` // 1=internal, 2=server, 3=client
	StartTime    string      `json:"startTimeUnixNano"`
	EndTime      string      `json:"endTimeUnixNano"`
	Attributes   []Attribute `json:"attributes"`
	Status       SpanStatus  `json:"status"`
}

// SpanStatus represents the status of a span.
type SpanStatus struct {
	Code int `json:"code"` // 0=unset, 1=ok, 2=error
}

// Attribute is a key-value pair in OTLP format.
type Attribute struct {
	Key   string         `json:"key"`
	Value AttributeValue `json:"value"`
}

// AttributeValue holds the typed value.
type AttributeValue struct {
	StringValue string `json:"stringValue,omitempty"`
	IntValue    string `json:"intValue,omitempty"`
}

// HTTPSpanInfo holds extracted HTTP info from a span.
type HTTPSpanInfo struct {
	ServiceName string
	Method      string
	Path        string
	StatusCode  string
	SpanKind    int
	DurationNs  int64
}

// IngestResult summarizes what the trace ingestion accomplished.
type IngestResult struct {
	SpansProcessed int `json:"spans_processed"`
	EdgesValidated int `json:"edges_validated"`
	EdgesEnriched  int `json:"edges_enriched"`
}

// Ingest reads an OTLP JSON file and uses the spans to validate/enrich HTTP_CALLS edges.
func Ingest(s *store.Store, project, filePath string) (*IngestResult, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read trace file: %w", err)
	}

	var export OTLPExport
	if err := json.Unmarshal(data, &export); err != nil {
		return nil, fmt.Errorf("parse OTLP JSON: %w", err)
	}

	result := &IngestResult{}

	var httpSpans []HTTPSpanInfo
	for _, rs := range export.ResourceSpans {
		serviceName := extractServiceName(rs.Resource)
		for _, ss := range rs.ScopeSpans {
			for i := range ss.Spans {
				info := extractHTTPInfo(&ss.Spans[i], serviceName)
				if info != nil {
					httpSpans = append(httpSpans, *info)
					result.SpansProcessed++
				}
			}
		}
	}

	slog.Info("traces.ingest", "http_spans", len(httpSpans))

	matchSpansToEdges(s, project, httpSpans, result)

	return result, nil
}

// extractServiceName gets service.name from resource attributes.
func extractServiceName(r Resource) string {
	for _, attr := range r.Attributes {
		if attr.Key == "service.name" {
			return attr.Value.StringValue
		}
	}
	return ""
}

// extractHTTPInfo extracts HTTP method/path from span attributes.
func extractHTTPInfo(span *Span, serviceName string) *HTTPSpanInfo {
	info := &HTTPSpanInfo{
		ServiceName: serviceName,
		SpanKind:    span.Kind,
	}

	hasHTTP := false
	for _, attr := range span.Attributes {
		switch attr.Key {
		case "http.method", "http.request.method":
			info.Method = attr.Value.StringValue
			hasHTTP = true
		case "http.route", "http.target", "url.path":
			info.Path = attr.Value.StringValue
			hasHTTP = true
		case "http.status_code":
			info.StatusCode = attr.Value.StringValue
		case "url.full":
			if path := extractPathFromURL(attr.Value.StringValue); path != "" {
				info.Path = path
				hasHTTP = true
			}
		}
	}

	if !hasHTTP || info.Path == "" {
		return nil
	}

	info.DurationNs = parseDuration(span.StartTime, span.EndTime)
	return info
}

// extractPathFromURL extracts the path component from a full URL string.
func extractPathFromURL(fullURL string) string {
	slashes := 0
	idx := 0
	for i, c := range fullURL {
		if c == '/' {
			slashes++
			if slashes == 3 {
				idx = i
				break
			}
		}
	}
	if idx > 0 {
		path := fullURL[idx:]
		if qIdx := strings.Index(path, "?"); qIdx >= 0 {
			path = path[:qIdx]
		}
		return path
	}
	return ""
}

// parseDuration parses nanosecond timestamps and returns duration.
func parseDuration(startNano, endNano string) int64 {
	var start, end int64
	_, _ = fmt.Sscanf(startNano, "%d", &start)
	_, _ = fmt.Sscanf(endNano, "%d", &end)
	if end > start {
		return end - start
	}
	return 0
}

// matchSpansToEdges matches HTTP spans to existing graph edges.
func matchSpansToEdges(s *store.Store, project string, spans []HTTPSpanInfo, result *IngestResult) {
	edges, err := s.FindEdgesByType(project, "HTTP_CALLS")
	if err != nil {
		slog.Warn("traces.edges.err", "err", err)
		return
	}

	edgeByPath := make(map[string]*store.Edge)
	for _, e := range edges {
		if urlPath, ok := e.Properties["url_path"].(string); ok {
			normalized := strings.ToLower(strings.TrimRight(urlPath, "/"))
			edgeByPath[normalized] = e
		}
	}

	pathFrequency := make(map[string]int)
	pathLatencies := make(map[string][]int64)

	for _, span := range spans {
		normalized := strings.ToLower(strings.TrimRight(span.Path, "/"))
		pathFrequency[normalized]++
		if span.DurationNs > 0 {
			pathLatencies[normalized] = append(pathLatencies[normalized], span.DurationNs)
		}
	}

	for path, edge := range edgeByPath {
		freq, ok := pathFrequency[path]
		if !ok {
			continue
		}

		result.EdgesValidated++

		props := edge.Properties
		if props == nil {
			props = make(map[string]any)
		}
		props["validated_by_trace"] = true
		props["trace_call_count"] = freq

		if conf, ok := props["confidence"].(float64); ok && conf < 0.9 {
			props["confidence"] = min(conf+0.15, 1.0)
			props["confidence_band"] = "high"
		}

		if latencies, ok := pathLatencies[path]; ok && len(latencies) > 0 {
			props["p99_latency_ns"] = calculateP99(latencies)
			result.EdgesEnriched++
		}

		edge.Properties = props
		_, _ = s.InsertEdge(edge)
	}
}

// calculateP99 returns the 99th percentile value.
func calculateP99(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * 0.99)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
