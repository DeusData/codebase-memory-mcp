package tools

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/metrics"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func TestParseFileURI(t *testing.T) {
	tests := []struct {
		uri      string
		wantPath string
		wantOK   bool
	}{
		// Unix paths
		{"file:///home/user/project", "/home/user/project", true},
		{"file:///tmp/test", "/tmp/test", true},

		// Windows paths — url.Parse returns /C:/path, must strip leading /
		{"file:///C:/Users/project", "C:/Users/project", true},
		{"file:///D:/Projects/myapp", "D:/Projects/myapp", true},

		// Non-file schemes
		{"https://example.com", "", false},
		{"", "", false},

		// Edge cases
		{"file:///", "/", true},
	}

	for _, tt := range tests {
		t.Run(tt.uri, func(t *testing.T) {
			got, ok := parseFileURI(tt.uri)
			if ok != tt.wantOK {
				t.Fatalf("parseFileURI(%q) ok=%v, want %v", tt.uri, ok, tt.wantOK)
			}
			if !ok {
				return
			}

			want := tt.wantPath
			// On Windows, filepath.FromSlash converts / to \
			if runtime.GOOS == "windows" {
				// paths will use backslashes
				want = windowsPath(want)
			}

			if got != want {
				t.Errorf("parseFileURI(%q) = %q, want %q", tt.uri, got, want)
			}
		})
	}
}

func TestPriceForConfig(t *testing.T) {
	tests := []struct {
		name  string
		cfg   func(t *testing.T) *store.ConfigStore
		want  float64
	}{
		{"nil config", func(t *testing.T) *store.ConfigStore { return nil }, 0.000015},
		{"default (claude-sonnet)", func(t *testing.T) *store.ConfigStore {
			c, err := store.OpenConfigInDir(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			return c
		}, 0.000015},
		{"claude-opus", func(t *testing.T) *store.ConfigStore {
			c, err := store.OpenConfigInDir(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			c.Set(store.ConfigPricingModel, "claude-opus")
			return c
		}, 0.000075},
		{"gpt-4o", func(t *testing.T) *store.ConfigStore {
			c, err := store.OpenConfigInDir(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			c.Set(store.ConfigPricingModel, "gpt-4o")
			return c
		}, 0.000010},
		{"custom", func(t *testing.T) *store.ConfigStore {
			c, err := store.OpenConfigInDir(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			c.Set(store.ConfigPricingModel, "custom")
			c.Set(store.ConfigCustomPricePerToken, "0.00005")
			return c
		}, 0.00005},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := priceForConfig(tt.cfg(t))
			if got != tt.want {
				t.Errorf("priceForConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitMetricsTracker(t *testing.T) {
	t.Run("default enabled", func(t *testing.T) {
		router, err := store.NewRouterWithDir(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(router.CloseAll)
		srv := NewServer(router)
		if srv.metricsTracker == nil {
			t.Error("expected metricsTracker to be non-nil by default")
		}
	})

	t.Run("disabled via config", func(t *testing.T) {
		router, err := store.NewRouterWithDir(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(router.CloseAll)
		cfg, err := store.OpenConfigInDir(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cfg.Close() })
		cfg.Set(store.ConfigMetricsEnabled, "false")

		srv := NewServer(router, WithConfig(cfg))
		if srv.metricsTracker != nil {
			t.Error("expected metricsTracker to be nil when disabled")
		}
	})

	t.Run("custom metrics_path via config", func(t *testing.T) {
		router, err := store.NewRouterWithDir(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(router.CloseAll)
		cfg, err := store.OpenConfigInDir(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cfg.Close() })

		customPath := filepath.Join(t.TempDir(), "custom-savings.json")
		cfg.Set(store.ConfigMetricsPath, customPath)

		srv := NewServer(router, WithConfig(cfg))
		if srv.metricsTracker == nil {
			t.Fatal("expected metricsTracker to be non-nil with custom path")
		}

		// Record something and verify it wrote to the custom path
		srv.metricsTracker.Record(metrics.TokenMetadata{TokensSaved: 100, CostAvoided: 0.001})
		if _, err := os.Stat(customPath); err != nil {
			t.Errorf("expected savings file at custom path %s: %v", customPath, err)
		}
	})
}

// windowsPath converts forward slashes to backslashes for Windows comparison.
func windowsPath(p string) string {
	result := make([]byte, len(p))
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			result[i] = '\\'
		} else {
			result[i] = p[i]
		}
	}
	return string(result)
}
