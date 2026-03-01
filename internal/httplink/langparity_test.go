package httplink

import (
	"strings"
	"testing"
)

func TestHTTPClientKeywordsAllLanguages(t *testing.T) {
	// Each language should have at least one keyword in httpClientKeywords.
	langKeywords := map[string][]string{
		"Python":     {"requests.get", "httpx.", "aiohttp."},
		"Go":         {"http.Get", "http.Post", "http.NewRequest"},
		"JavaScript": {"fetch(", "axios."},
		"TypeScript": {"fetch(", "axios."},
		"Java":       {"HttpClient", "RestTemplate"},
		"Rust":       {"reqwest::", "hyper::"},
		"PHP":        {"curl_exec", "Guzzle"},
		"Scala":      {"sttp.", "http4s"},
		"CPP":        {"curl_easy", "cpr::Get"},
		"Lua":        {"socket.http", "http.request"},
		"CSharp":     {"HttpClient", "WebClient", "RestClient"},
		"Kotlin":     {"HttpClient", "OkHttpClient"},
	}

	joined := strings.Join(httpClientKeywords, "|")

	for langName, keywords := range langKeywords {
		t.Run(langName, func(t *testing.T) {
			found := false
			for _, kw := range keywords {
				if strings.Contains(joined, kw) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("no HTTP client keywords found for %s; want at least one of %v", langName, keywords)
			}
		})
	}
}

func TestRouteExtractionNegativeCases(t *testing.T) {
	// Non-route functions should NOT produce spurious route matches.
	tests := []struct {
		name   string
		source string
	}{
		{"plain_function", "func processOrder(order Order) error {\n\treturn nil\n}\n"},
		{"math_function", "function calculate(x, y) {\n\treturn x + y;\n}\n"},
		{"utility", "def transform_data(data):\n    return data.upper()\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Python route patterns
			matches := pyRouteRe.FindAllStringSubmatch(tt.source, -1)
			if len(matches) > 0 {
				t.Errorf("pyRouteRe matched non-route source: %v", matches)
			}

			// Go route patterns
			matches = goRouteRe.FindAllStringSubmatch(tt.source, -1)
			if len(matches) > 0 {
				t.Errorf("goRouteRe matched non-route source: %v", matches)
			}

			// Express route patterns
			matches = expressRouteRe.FindAllStringSubmatch(tt.source, -1)
			if len(matches) > 0 {
				t.Errorf("expressRouteRe matched non-route source: %v", matches)
			}

			// Spring route patterns
			matches = springMappingRe.FindAllStringSubmatch(tt.source, -1)
			if len(matches) > 0 {
				t.Errorf("springMappingRe matched non-route source: %v", matches)
			}

			// Laravel route patterns
			matches = laravelRouteRe.FindAllStringSubmatch(tt.source, -1)
			if len(matches) > 0 {
				t.Errorf("laravelRouteRe matched non-route source: %v", matches)
			}

			// Actix route patterns
			matches = actixRouteRe.FindAllStringSubmatch(tt.source, -1)
			if len(matches) > 0 {
				t.Errorf("actixRouteRe matched non-route source: %v", matches)
			}
		})
	}
}
