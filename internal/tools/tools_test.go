package tools

import (
	"runtime"
	"testing"
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
