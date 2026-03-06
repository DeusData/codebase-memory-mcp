package pipeline

import "testing"

func TestParseGoReceiver(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantVar  string
		wantType string
	}{
		{
			name:     "pointer receiver",
			input:    "(h *Handler)",
			wantVar:  "h",
			wantType: "Handler",
		},
		{
			name:     "value receiver",
			input:    "(s MyService)",
			wantVar:  "s",
			wantType: "MyService",
		},
		{
			name:     "empty string",
			input:    "",
			wantVar:  "",
			wantType: "",
		},
		{
			name:     "single word no pair",
			input:    "invalid",
			wantVar:  "",
			wantType: "",
		},
		{
			name:     "too many parts",
			input:    "(a b c)",
			wantVar:  "",
			wantType: "",
		},
		{
			name:     "empty parens",
			input:    "()",
			wantVar:  "",
			wantType: "",
		},
		{
			name:     "whitespace only parens",
			input:    "( )",
			wantVar:  "",
			wantType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVar, gotType := parseGoReceiver(tt.input)
			if gotVar != tt.wantVar {
				t.Errorf("parseGoReceiver(%q) varName = %q, want %q", tt.input, gotVar, tt.wantVar)
			}
			if gotType != tt.wantType {
				t.Errorf("parseGoReceiver(%q) typeName = %q, want %q", tt.input, gotType, tt.wantType)
			}
		})
	}
}
