package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// testBinPath is set in TestMain — persists across all tests in this package.
var testBinPath string

func TestMain(m *testing.M) {
	// Build the binary once into a temp dir that persists for the full test run.
	tmpDir, err := os.MkdirTemp("", "cmm-cli-test-*")
	if err != nil {
		panic("create temp dir: " + err.Error())
	}

	binName := "codebase-memory-mcp"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(tmpDir, binName)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binPath, "./")
	cmd.Dir = "."
	if out, err := cmd.CombinedOutput(); err != nil {
		cancel()
		os.RemoveAll(tmpDir)
		os.Stderr.Write(out)
		panic("build test binary: " + err.Error())
	}
	cancel()
	testBinPath = binPath

	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

func testCmd(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return exec.CommandContext(ctx, testBinPath, args...)
}

// testEnvWithHome returns env vars with HOME (and USERPROFILE on Windows) set.
func testEnvWithHome(home string, extra ...string) []string {
	env := append(os.Environ(), "HOME="+home)
	if runtime.GOOS == "windows" {
		env = append(env, "USERPROFILE="+home)
	}
	return append(env, extra...)
}

func TestCLI_Version(t *testing.T) {
	out, err := testCmd(t, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("--version failed: %v\n%s", err, out)
	}
	output := strings.TrimSpace(string(out))
	if !strings.HasPrefix(output, "codebase-memory-mcp") {
		t.Fatalf("unexpected --version output: %q", output)
	}
}

func TestCLI_InstallDryRun(t *testing.T) {
	home := t.TempDir()
	cmd := testCmd(t, "install", "--dry-run")
	cmd.Env = testEnvWithHome(home, "PATH="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install --dry-run failed: %v\n%s", err, out)
	}
	output := string(out)
	if !strings.Contains(output, "install") {
		t.Fatalf("expected 'install' in output, got: %s", output)
	}
	// Dry run should not create any files
	skillsDir := filepath.Join(home, ".claude", "skills")
	if _, err := os.Stat(skillsDir); !os.IsNotExist(err) {
		t.Fatal("dry-run should not create skills directory")
	}
}

func TestCLI_UninstallDryRun(t *testing.T) {
	home := t.TempDir()
	cmd := testCmd(t, "uninstall", "--dry-run")
	cmd.Env = testEnvWithHome(home, "PATH="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("uninstall --dry-run failed: %v\n%s", err, out)
	}
	output := string(out)
	if !strings.Contains(output, "uninstall") {
		t.Fatalf("expected 'uninstall' in output, got: %s", output)
	}
}

func TestCLI_UpdateDryRun(t *testing.T) {
	cmd := testCmd(t, "update", "--dry-run")
	out, _ := cmd.CombinedOutput()
	output := string(out)
	if !strings.Contains(output, "checking for updates") {
		t.Fatalf("expected 'checking for updates' in output, got: %s", output)
	}
}

func TestCLI_CliHelp(t *testing.T) {
	cmd := testCmd(t, "cli", "--help")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cli --help failed: %v\n%s", err, out)
	}
	output := string(out)
	if !strings.Contains(output, "Available tools") {
		t.Fatalf("expected 'Available tools' in cli --help output, got: %s", output)
	}
}

func TestCLI_InstallAndUninstall(t *testing.T) {
	home := t.TempDir()
	emptyPath := t.TempDir()

	// Install
	cmd := testCmd(t, "install")
	cmd.Env = testEnvWithHome(home, "PATH="+emptyPath, "SHELL=/bin/zsh")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install failed: %v\n%s", err, out)
	}

	// Verify skills were created
	expectedSkills := []string{
		"codebase-memory-exploring",
		"codebase-memory-tracing",
		"codebase-memory-quality",
		"codebase-memory-reference",
	}
	for _, name := range expectedSkills {
		skillFile := filepath.Join(home, ".claude", "skills", name, "SKILL.md")
		if _, err := os.Stat(skillFile); err != nil {
			t.Fatalf("skill %s not found after install: %v", name, err)
		}
	}

	// Uninstall
	cmd = testCmd(t, "uninstall")
	cmd.Env = testEnvWithHome(home, "PATH="+emptyPath)
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("uninstall failed: %v\n%s", err, out)
	}

	// Verify skills were removed
	for _, name := range expectedSkills {
		skillDir := filepath.Join(home, ".claude", "skills", name)
		if _, err := os.Stat(skillDir); !os.IsNotExist(err) {
			t.Fatalf("skill dir %s should be removed after uninstall", name)
		}
	}
}

func TestCLI_InstallRemovesOldSkill(t *testing.T) {
	home := t.TempDir()
	emptyPath := t.TempDir()

	oldDir := filepath.Join(home, ".claude", "skills", "codebase-memory-mcp")
	if err := os.MkdirAll(oldDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldDir, "SKILL.md"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := testCmd(t, "install")
	cmd.Env = testEnvWithHome(home, "PATH="+emptyPath, "SHELL=/bin/zsh")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install failed: %v\n%s", err, out)
	}

	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatal("old monolithic skill dir should be removed")
	}
	if _, err := os.Stat(filepath.Join(home, ".claude", "skills", "codebase-memory-exploring", "SKILL.md")); err != nil {
		t.Fatal("new exploring skill should exist")
	}
}

func TestCLI_InstallIdempotent(t *testing.T) {
	home := t.TempDir()
	emptyPath := t.TempDir()

	for i := 0; i < 2; i++ {
		cmd := testCmd(t, "install")
		cmd.Env = testEnvWithHome(home, "PATH="+emptyPath, "SHELL=/bin/zsh")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("install round %d failed: %v\n%s", i, err, out)
		}
	}

	skillFile := filepath.Join(home, ".claude", "skills", "codebase-memory-exploring", "SKILL.md")
	if _, err := os.Stat(skillFile); err != nil {
		t.Fatal("skill missing after idempotent install")
	}
}

func TestCLI_InstallForceOverwrites(t *testing.T) {
	home := t.TempDir()
	emptyPath := t.TempDir()

	cmd := testCmd(t, "install")
	cmd.Env = testEnvWithHome(home, "PATH="+emptyPath, "SHELL=/bin/zsh")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("first install failed: %v\n%s", err, out)
	}

	skillFile := filepath.Join(home, ".claude", "skills", "codebase-memory-exploring", "SKILL.md")
	if err := os.WriteFile(skillFile, []byte("custom content"), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd = testCmd(t, "install")
	cmd.Env = testEnvWithHome(home, "PATH="+emptyPath, "SHELL=/bin/zsh")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("second install failed: %v\n%s", err, out)
	}
	data, _ := os.ReadFile(skillFile)
	if string(data) != "custom content" {
		t.Fatal("install without --force should not overwrite customized skills")
	}

	cmd = testCmd(t, "install", "--force")
	cmd.Env = testEnvWithHome(home, "PATH="+emptyPath, "SHELL=/bin/zsh")
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("force install failed: %v\n%s", err, out)
	}
	data, _ = os.ReadFile(skillFile)
	if string(data) == "custom content" {
		t.Fatal("install --force should overwrite customized skills")
	}
}

func TestCLI_InstallPATHAppend(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell RC PATH append is Unix-specific")
	}

	home := t.TempDir()
	emptyPath := t.TempDir()

	cmd := testCmd(t, "install")
	cmd.Env = testEnvWithHome(home, "PATH="+emptyPath, "SHELL=/bin/zsh")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install failed: %v\n%s", err, out)
	}

	zshrc := filepath.Join(home, ".zshrc")
	data, err := os.ReadFile(zshrc)
	if err != nil {
		t.Fatalf("expected .zshrc to be created: %v", err)
	}
	if !strings.Contains(string(data), "export PATH=") {
		t.Fatal("expected PATH export in .zshrc")
	}
	if !strings.Contains(string(data), "codebase-memory-mcp install") {
		t.Fatal("expected install comment in .zshrc")
	}
}
