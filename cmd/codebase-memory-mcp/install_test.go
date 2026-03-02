package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// setTestHome overrides the home directory for both Unix (HOME) and Windows (USERPROFILE).
func setTestHome(t *testing.T, home string) {
	t.Helper()
	t.Setenv("HOME", home)
	if runtime.GOOS == "windows" {
		t.Setenv("USERPROFILE", home)
	}
}

// exeSuffix returns ".exe" on Windows, empty string otherwise.
func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func TestInstallSkillCreation(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	claudeSkillsDir := filepath.Join(home, ".claude", "skills")

	for name, content := range skillFiles {
		skillDir := filepath.Join(claudeSkillsDir, name)
		skillFile := filepath.Join(skillDir, "SKILL.md")

		if err := os.MkdirAll(skillDir, 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", skillDir, err)
		}
		if err := os.WriteFile(skillFile, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", skillFile, err)
		}
	}

	expectedSkills := []string{
		"codebase-memory-exploring",
		"codebase-memory-tracing",
		"codebase-memory-quality",
		"codebase-memory-reference",
	}

	for _, name := range expectedSkills {
		skillFile := filepath.Join(claudeSkillsDir, name, "SKILL.md")
		data, err := os.ReadFile(skillFile)
		if err != nil {
			t.Fatalf("read %s: %v", skillFile, err)
		}
		if len(data) == 0 {
			t.Fatalf("skill file %s is empty", skillFile)
		}
		normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
		if !strings.HasPrefix(normalized, "---\n") {
			t.Fatalf("skill %s missing YAML frontmatter", name)
		}
		if !strings.Contains(string(data), "name: "+name) {
			t.Fatalf("skill %s doesn't contain correct name field", name)
		}
	}
}

func TestInstallIdempotent(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	claudeSkillsDir := filepath.Join(home, ".claude", "skills")

	for round := 0; round < 2; round++ {
		for name, content := range skillFiles {
			skillDir := filepath.Join(claudeSkillsDir, name)
			skillFile := filepath.Join(skillDir, "SKILL.md")
			if err := os.MkdirAll(skillDir, 0o750); err != nil {
				t.Fatalf("round %d: mkdir %s: %v", round, skillDir, err)
			}
			if err := os.WriteFile(skillFile, []byte(content), 0o600); err != nil {
				t.Fatalf("round %d: write %s: %v", round, skillFile, err)
			}
		}
	}

	for name := range skillFiles {
		skillFile := filepath.Join(claudeSkillsDir, name, "SKILL.md")
		if _, err := os.Stat(skillFile); err != nil {
			t.Fatalf("skill %s missing after idempotent install: %v", name, err)
		}
	}
}

func TestUninstallRemovesSkills(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	claudeSkillsDir := filepath.Join(home, ".claude", "skills")

	for name, content := range skillFiles {
		skillDir := filepath.Join(claudeSkillsDir, name)
		if err := os.MkdirAll(skillDir, 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(content), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	for name := range skillFiles {
		skillDir := filepath.Join(claudeSkillsDir, name)
		if err := os.RemoveAll(skillDir); err != nil {
			t.Fatalf("remove %s: %v", skillDir, err)
		}
	}

	for name := range skillFiles {
		skillDir := filepath.Join(claudeSkillsDir, name)
		if _, err := os.Stat(skillDir); !os.IsNotExist(err) {
			t.Fatalf("skill dir %s should not exist after uninstall", skillDir)
		}
	}
}

func TestFindCLI_NotFound(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	setTestHome(t, t.TempDir())

	result := findCLI("nonexistent-binary-xyz")
	if result != "" {
		t.Fatalf("expected empty string for nonexistent CLI, got %q", result)
	}
}

func TestFindCLI_FoundOnPATH(t *testing.T) {
	tmpDir := t.TempDir()

	fakeBin := filepath.Join(tmpDir, "fakecli"+exeSuffix())
	if err := os.WriteFile(fakeBin, []byte("#!/bin/sh\n"), 0o600); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	if err := os.Chmod(fakeBin, 0o500); err != nil {
		t.Fatalf("chmod fake binary: %v", err)
	}

	t.Setenv("PATH", tmpDir)
	result := findCLI("fakecli" + exeSuffix())
	if result == "" {
		t.Fatal("expected to find fakecli on PATH")
	}
}

func TestFindCLI_FallbackPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fallback paths use Unix-specific locations")
	}

	home := t.TempDir()
	setTestHome(t, home)
	t.Setenv("PATH", t.TempDir())

	localBin := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(localBin, 0o750); err != nil {
		t.Fatal(err)
	}
	fakeBin := filepath.Join(localBin, "testcli")
	if err := os.WriteFile(fakeBin, []byte("#!/bin/sh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(fakeBin, 0o500); err != nil {
		t.Fatal(err)
	}

	result := findCLI("testcli")
	if result != fakeBin {
		t.Fatalf("expected %q, got %q", fakeBin, result)
	}
}

func TestDetectBinaryPath(t *testing.T) {
	path, err := detectBinaryPath()
	if err != nil {
		t.Fatalf("detectBinaryPath error: %v", err)
	}
	if path == "" {
		t.Fatal("detectBinaryPath returned empty string")
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("expected absolute path, got %q", path)
	}
}

func TestDetectShellRC(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell RC detection is Unix-specific")
	}

	home := t.TempDir()
	setTestHome(t, home)

	tests := []struct {
		shell    string
		expected string
	}{
		{"/bin/zsh", ".zshrc"},
		{"/bin/bash", ".bash_profile"},
		{"/usr/bin/fish", filepath.Join(".config", "fish", "config.fish")},
		{"/bin/sh", ".profile"},
	}

	for _, tt := range tests {
		t.Run(tt.shell, func(t *testing.T) {
			t.Setenv("SHELL", tt.shell)
			rc := detectShellRC()
			if rc == "" {
				t.Fatal("detectShellRC returned empty")
			}
			if !strings.HasSuffix(rc, tt.expected) {
				t.Fatalf("for shell %q: got %q, want suffix %q", tt.shell, rc, tt.expected)
			}
		})
	}
}

func TestDetectShellRC_BashWithBashrc(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell RC detection is Unix-specific")
	}

	home := t.TempDir()
	setTestHome(t, home)
	t.Setenv("SHELL", "/bin/bash")

	bashrc := filepath.Join(home, ".bashrc")
	if err := os.WriteFile(bashrc, []byte("# test\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	rc := detectShellRC()
	if rc != bashrc {
		t.Fatalf("expected %q, got %q", bashrc, rc)
	}
}

func TestDryRun(t *testing.T) {
	cfg := installConfig{}
	for _, a := range []string{"--dry-run", "--force"} {
		switch a {
		case "--dry-run":
			cfg.dryRun = true
		case "--force":
			cfg.force = true
		}
	}
	if !cfg.dryRun || !cfg.force {
		t.Fatal("expected both dryRun and force to be true")
	}
}

func TestCodexInstructionsCreation(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	instrDir := filepath.Join(home, ".codex", "instructions")
	instrFile := filepath.Join(instrDir, "codebase-memory-mcp.md")

	if err := os.MkdirAll(instrDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(instrFile, []byte(codexInstructions), 0o600); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(instrFile)
	if err != nil {
		t.Fatalf("read instructions: %v", err)
	}
	if !strings.Contains(string(data), "Codebase Knowledge Graph") {
		t.Fatal("instructions file missing expected content")
	}
	if !strings.Contains(string(data), "trace_call_path") {
		t.Fatal("instructions file missing trace_call_path reference")
	}
}

func TestSkillFilesContent(t *testing.T) {
	if len(skillFiles) != 4 {
		t.Fatalf("expected 4 skill files, got %d", len(skillFiles))
	}

	expectations := map[string][]string{
		"codebase-memory-exploring": {"explore the codebase", "search_graph", "get_graph_schema"},
		"codebase-memory-tracing":   {"who calls this function", "trace_call_path", "direction", "risk_labels", "detect_changes"},
		"codebase-memory-quality":   {"find dead code", "max_degree=0", "exclude_entry_points"},
		"codebase-memory-reference": {"edge types", "query_graph", "Cypher", "detect_changes", "14 total"},
	}

	for name, expectedPhrases := range expectations {
		content, ok := skillFiles[name]
		if !ok {
			t.Fatalf("missing skill: %s", name)
		}
		for _, phrase := range expectedPhrases {
			if !strings.Contains(strings.ToLower(content), strings.ToLower(phrase)) {
				t.Errorf("skill %q missing phrase %q", name, phrase)
			}
		}
	}
}

func TestEditorMCPInstall(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	configPath := filepath.Join(home, ".cursor", "mcp.json")
	binaryPath := "/usr/local/bin/codebase-memory-mcp"

	// First install — creates file from scratch
	installEditorMCP(binaryPath, configPath, "Cursor", installConfig{})

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	servers, ok := root["mcpServers"].(map[string]any)
	if !ok {
		t.Fatal("expected mcpServers key")
	}
	entry, ok := servers["codebase-memory-mcp"].(map[string]any)
	if !ok {
		t.Fatal("expected codebase-memory-mcp entry")
	}
	if cmd, _ := entry["command"].(string); cmd != binaryPath {
		t.Fatalf("expected command %q, got %q", binaryPath, cmd)
	}
}

func TestEditorMCPInstallIdempotent(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	configPath := filepath.Join(home, ".cursor", "mcp.json")
	binaryPath := "/usr/local/bin/codebase-memory-mcp"

	// Install twice — second install should preserve valid JSON
	installEditorMCP(binaryPath, configPath, "Cursor", installConfig{})
	installEditorMCP(binaryPath, configPath, "Cursor", installConfig{})

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("double install produced invalid JSON: %v", err)
	}
	servers, ok := root["mcpServers"].(map[string]any)
	if !ok {
		t.Fatal("mcpServers is not a map")
	}
	if len(servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(servers))
	}
}

func TestEditorMCPPreservesOtherServers(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	configPath := filepath.Join(home, ".cursor", "mcp.json")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o750); err != nil {
		t.Fatal(err)
	}

	// Write config with an existing server
	existing := `{"mcpServers": {"other-server": {"command": "/usr/bin/other"}}}`
	if err := os.WriteFile(configPath, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	installEditorMCP("/usr/local/bin/codebase-memory-mcp", configPath, "Cursor", installConfig{})

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	servers, ok := root["mcpServers"].(map[string]any)
	if !ok {
		t.Fatal("mcpServers is not a map")
	}
	if len(servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(servers))
	}
	if _, ok = servers["other-server"]; !ok {
		t.Fatal("other-server was removed")
	}
	if _, ok := servers["codebase-memory-mcp"]; !ok {
		t.Fatal("codebase-memory-mcp not added")
	}
}

func TestEditorMCPUninstall(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	configPath := filepath.Join(home, ".cursor", "mcp.json")

	// Install then uninstall
	installEditorMCP("/usr/local/bin/codebase-memory-mcp", configPath, "Cursor", installConfig{})
	removeEditorMCP(configPath, "Cursor", installConfig{})

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	servers, ok := root["mcpServers"].(map[string]any)
	if !ok {
		t.Fatal("mcpServers is not a map")
	}
	if _, exists := servers["codebase-memory-mcp"]; exists {
		t.Fatal("codebase-memory-mcp should be removed after uninstall")
	}
}

func TestRemoveOldMonolithicSkill(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)

	oldDir := filepath.Join(home, ".claude", "skills", "codebase-memory-mcp")
	if err := os.MkdirAll(oldDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldDir, "SKILL.md"), []byte("old skill"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(oldDir); err != nil {
		t.Fatalf("remove old skill: %v", err)
	}

	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatal("old monolithic skill dir should be removed")
	}
}
