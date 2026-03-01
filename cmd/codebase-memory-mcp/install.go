package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// installConfig holds settings for the install/uninstall commands.
type installConfig struct {
	dryRun bool
	force  bool
}

func runInstall(args []string) int {
	cfg := installConfig{}
	for _, a := range args {
		switch a {
		case "--dry-run":
			cfg.dryRun = true
		case "--force":
			cfg.force = true
		}
	}

	binaryPath, err := detectBinaryPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	fmt.Printf("\ncodebase-memory-mcp %s — install\n", version)
	fmt.Printf("Binary: %s\n\n", binaryPath)

	// PATH check
	ensurePATH(binaryPath, cfg)

	// Skills (always installed — no CLI dependency)
	installSkills(cfg)

	// Claude Code MCP registration
	if claudePath := findCLI("claude"); claudePath != "" {
		fmt.Printf("[Claude Code] detected (%s)\n", claudePath)
		registerClaudeCodeMCP(binaryPath, claudePath, cfg)
	} else {
		fmt.Println("[Claude Code] not found — skipping MCP registration")
	}

	fmt.Println()

	// Codex CLI
	if codexPath := findCLI("codex"); codexPath != "" {
		fmt.Printf("[Codex CLI] detected (%s)\n", codexPath)
		installCodex(binaryPath, codexPath, cfg)
	} else {
		fmt.Println("[Codex CLI] not found — skipping")
	}

	fmt.Println("\nDone. Restart Claude Code / Codex to activate.")
	return 0
}

func runUninstall(args []string) int {
	cfg := installConfig{}
	for _, a := range args {
		if a == "--dry-run" {
			cfg.dryRun = true
		}
	}

	fmt.Printf("\ncodebase-memory-mcp %s — uninstall\n\n", version)

	// Remove Claude Code skills
	removeClaudeSkills(cfg)

	// Claude Code MCP deregistration
	if claudePath := findCLI("claude"); claudePath != "" {
		fmt.Printf("[Claude Code] detected (%s)\n", claudePath)
		deregisterMCP(claudePath, "claude", cfg)
	}

	// Codex CLI MCP deregistration + instructions
	if codexPath := findCLI("codex"); codexPath != "" {
		fmt.Printf("[Codex CLI] detected (%s)\n", codexPath)
		removeCodexMCP(cfg)
		removeCodexInstructions(cfg)
	}

	fmt.Println("\nDone. Binary and databases were NOT removed.")
	return 0
}

// detectBinaryPath resolves the current binary's real path.
func detectBinaryPath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("detect binary: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return "", fmt.Errorf("resolve symlink: %w", err)
	}
	return resolved, nil
}

// ensurePATH checks if the binary directory is on PATH and offers to add it.
func ensurePATH(binaryPath string, cfg installConfig) {
	binDir := filepath.Dir(binaryPath)
	pathDirs := filepath.SplitList(os.Getenv("PATH"))

	fmt.Println("[PATH]")
	for _, d := range pathDirs {
		if d == binDir {
			fmt.Printf("  ✓ %s already on PATH\n", binDir)
			return
		}
	}

	fmt.Printf("  ⚠ %s is not on PATH\n", binDir)

	if runtime.GOOS == "windows" {
		fmt.Printf("  → Add %s to your PATH environment variable manually\n", binDir)
		return
	}

	rcFile := detectShellRC()
	if rcFile == "" {
		fmt.Printf("  → Add to your shell profile: export PATH=\"%s:$PATH\"\n", binDir)
		return
	}

	line := fmt.Sprintf("export PATH=\"%s:$PATH\"", binDir)

	// Check if already present in rc file
	if content, err := os.ReadFile(rcFile); err == nil {
		if strings.Contains(string(content), line) {
			fmt.Printf("  ✓ Already in %s (restart terminal to activate)\n", rcFile)
			return
		}
	}

	if cfg.dryRun {
		fmt.Printf("  [dry-run] Would append to %s: %s\n", rcFile, line)
	} else {
		f, err := os.OpenFile(rcFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Printf("  ⚠ Could not write to %s: %v\n", rcFile, err)
			fmt.Printf("  → Add manually: %s\n", line)
			return
		}
		defer f.Close()
		fmt.Fprintf(f, "\n# Added by codebase-memory-mcp install\n%s\n", line)
		fmt.Printf("  ✓ Added to %s: %s\n", rcFile, line)
		fmt.Printf("  → Run: source %s (or restart terminal)\n", rcFile)
	}
}

// detectShellRC returns the appropriate shell rc file path.
func detectShellRC() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	shell := os.Getenv("SHELL")
	switch {
	case strings.HasSuffix(shell, "/zsh"):
		return filepath.Join(home, ".zshrc")
	case strings.HasSuffix(shell, "/bash"):
		// Prefer .bashrc, fall back to .bash_profile
		bashrc := filepath.Join(home, ".bashrc")
		if _, err := os.Stat(bashrc); err == nil {
			return bashrc
		}
		return filepath.Join(home, ".bash_profile")
	case strings.HasSuffix(shell, "/fish"):
		return filepath.Join(home, ".config", "fish", "config.fish")
	default:
		// Fall back to .profile
		return filepath.Join(home, ".profile")
	}
}

// installSkills writes the 4 skill files to ~/.claude/skills/ and removes old monolithic skill.
func installSkills(cfg installConfig) {
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("  ⚠ Cannot determine home directory: %v\n", err)
		return
	}

	fmt.Println("[Skills]")

	// Remove old monolithic skill if it exists
	oldSkillDir := filepath.Join(home, ".claude", "skills", "codebase-memory-mcp")
	if info, err := os.Stat(oldSkillDir); err == nil && info.IsDir() {
		if cfg.dryRun {
			fmt.Printf("  [dry-run] Would remove old skill: %s\n", oldSkillDir)
		} else {
			if err := os.RemoveAll(oldSkillDir); err == nil {
				fmt.Printf("  ✓ Removed old monolithic skill: %s\n", oldSkillDir)
			}
		}
	}

	// Write 4 skill files
	for name, content := range skillFiles {
		skillDir := filepath.Join(home, ".claude", "skills", name)
		skillFile := filepath.Join(skillDir, "SKILL.md")

		if !cfg.force {
			if _, err := os.Stat(skillFile); err == nil {
				fmt.Printf("  ✓ Skill exists (skip): %s\n", skillFile)
				continue
			}
		}

		if cfg.dryRun {
			fmt.Printf("  [dry-run] Would write: %s\n", skillFile)
			continue
		}

		if err := os.MkdirAll(skillDir, 0o750); err != nil {
			fmt.Printf("  ⚠ mkdir %s: %v\n", skillDir, err)
			continue
		}
		if err := os.WriteFile(skillFile, []byte(content), 0o600); err != nil {
			fmt.Printf("  ⚠ write %s: %v\n", skillFile, err)
			continue
		}
		fmt.Printf("  ✓ Skill: %s\n", skillFile)
	}
}

// registerClaudeCodeMCP registers the MCP server with Claude Code CLI.
func registerClaudeCodeMCP(binaryPath, claudePath string, cfg installConfig) {
	if cfg.dryRun {
		fmt.Printf("  [dry-run] Would run: %s mcp remove -s user codebase-memory-mcp\n", claudePath)
		fmt.Printf("  [dry-run] Would run: %s mcp add --scope user codebase-memory-mcp -- %s\n", claudePath, binaryPath)
	} else {
		// Silent remove (may fail if not registered — that's fine)
		_ = execCLI(claudePath, "mcp", "remove", "-s", "user", "codebase-memory-mcp")
		if err := execCLI(claudePath, "mcp", "add", "--scope", "user", "codebase-memory-mcp", "--", binaryPath); err != nil {
			fmt.Printf("  ⚠ MCP registration failed: %v\n", err)
		} else {
			fmt.Println("  ✓ MCP server registered (scope: user)")
		}
	}
}

// installCodex installs MCP registration and instructions for Codex CLI.
func installCodex(binaryPath, _ string, cfg installConfig) {
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("  ⚠ Cannot determine home directory: %v\n", err)
		return
	}

	// Register MCP server via config.toml
	configFile := filepath.Join(home, ".codex", "config.toml")
	mcpSection := fmt.Sprintf("\n[mcp_servers.codebase-memory-mcp]\ncommand = %q\n", binaryPath)

	if cfg.dryRun {
		fmt.Printf("  [dry-run] Would add MCP server to: %s\n", configFile)
	} else {
		if err := os.MkdirAll(filepath.Dir(configFile), 0o750); err != nil {
			fmt.Printf("  ⚠ mkdir %s: %v\n", filepath.Dir(configFile), err)
		} else if err := upsertCodexMCP(configFile, mcpSection, binaryPath); err != nil {
			fmt.Printf("  ⚠ MCP registration failed: %v\n", err)
		} else {
			fmt.Printf("  ✓ MCP server registered: %s\n", configFile)
		}
	}

	// Write instructions file
	instrDir := filepath.Join(home, ".codex", "instructions")
	instrFile := filepath.Join(instrDir, "codebase-memory-mcp.md")

	if cfg.dryRun {
		fmt.Printf("  [dry-run] Would write: %s\n", instrFile)
	} else {
		if err := os.MkdirAll(instrDir, 0o750); err != nil {
			fmt.Printf("  ⚠ mkdir %s: %v\n", instrDir, err)
			return
		}
		if err := os.WriteFile(instrFile, []byte(codexInstructions), 0o600); err != nil {
			fmt.Printf("  ⚠ write %s: %v\n", instrFile, err)
			return
		}
		fmt.Printf("  ✓ Instructions: %s\n", instrFile)
	}
}

// upsertCodexMCP adds or updates the codebase-memory-mcp section in config.toml.
func upsertCodexMCP(configFile, mcpSection, binaryPath string) error {
	content, err := os.ReadFile(configFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	text := string(content)

	// If section already exists, replace the command line
	const sectionHeader = "[mcp_servers.codebase-memory-mcp]"
	if idx := strings.Index(text, sectionHeader); idx >= 0 {
		// Find the end of this section (next [ or EOF)
		rest := text[idx+len(sectionHeader):]
		endIdx := strings.Index(rest, "\n[")
		if endIdx < 0 {
			endIdx = len(rest)
		}
		newSection := fmt.Sprintf("%s\ncommand = %q\n", sectionHeader, binaryPath)
		text = text[:idx] + newSection + rest[endIdx:]
	} else {
		// Append new section
		text += mcpSection
	}

	return os.WriteFile(configFile, []byte(text), 0o600)
}

// removeClaudeSkills removes all 4 skill directories.
func removeClaudeSkills(cfg installConfig) {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}

	fmt.Println("[Skills]")
	for name := range skillFiles {
		skillDir := filepath.Join(home, ".claude", "skills", name)
		if _, err := os.Stat(skillDir); os.IsNotExist(err) {
			continue
		}
		if cfg.dryRun {
			fmt.Printf("  [dry-run] Would remove: %s\n", skillDir)
		} else {
			if err := os.RemoveAll(skillDir); err != nil {
				fmt.Printf("  ⚠ remove %s: %v\n", skillDir, err)
			} else {
				fmt.Printf("  ✓ Removed: %s\n", skillDir)
			}
		}
	}
}

// deregisterMCP removes the MCP server registration from a CLI.
func deregisterMCP(cliPath, cliName string, cfg installConfig) {
	if cfg.dryRun {
		fmt.Printf("  [dry-run] Would run: %s mcp remove -s user codebase-memory-mcp\n", cliPath)
	} else {
		if err := execCLI(cliPath, "mcp", "remove", "-s", "user", "codebase-memory-mcp"); err != nil {
			fmt.Printf("  ⚠ %s MCP deregistration: %v\n", cliName, err)
		} else {
			fmt.Printf("  ✓ %s MCP server deregistered\n", cliName)
		}
	}
}

// removeCodexMCP removes the codebase-memory-mcp section from Codex config.toml.
func removeCodexMCP(cfg installConfig) {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	configFile := filepath.Join(home, ".codex", "config.toml")
	content, err := os.ReadFile(configFile)
	if err != nil {
		return
	}

	text := string(content)
	const sectionHeader = "[mcp_servers.codebase-memory-mcp]"
	idx := strings.Index(text, sectionHeader)
	if idx < 0 {
		return
	}

	if cfg.dryRun {
		fmt.Printf("  [dry-run] Would remove MCP section from: %s\n", configFile)
		return
	}

	// Find end of section (next [ or EOF)
	rest := text[idx+len(sectionHeader):]
	endIdx := strings.Index(rest, "\n[")
	if endIdx < 0 {
		text = strings.TrimRight(text[:idx], "\n")
	} else {
		text = text[:idx] + rest[endIdx+1:]
	}

	if err := os.WriteFile(configFile, []byte(text), 0o600); err != nil {
		fmt.Printf("  ⚠ update %s: %v\n", configFile, err)
	} else {
		fmt.Printf("  ✓ Removed MCP section from: %s\n", configFile)
	}
}

// removeCodexInstructions removes the Codex instructions file.
func removeCodexInstructions(cfg installConfig) {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	instrFile := filepath.Join(home, ".codex", "instructions", "codebase-memory-mcp.md")
	if _, err := os.Stat(instrFile); os.IsNotExist(err) {
		return
	}
	if cfg.dryRun {
		fmt.Printf("  [dry-run] Would remove: %s\n", instrFile)
	} else {
		if err := os.Remove(instrFile); err != nil {
			fmt.Printf("  ⚠ remove %s: %v\n", instrFile, err)
		} else {
			fmt.Printf("  ✓ Removed: %s\n", instrFile)
		}
	}
}

// findCLI locates a CLI binary by name.
func findCLI(name string) string {
	if p, err := exec.LookPath(name); err == nil {
		return p
	}

	// Check common install locations
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	candidates := []string{
		"/usr/local/bin/" + name,
		filepath.Join(home, ".npm", "bin", name),
		filepath.Join(home, ".local", "bin", name),
		filepath.Join(home, ".cargo", "bin", name),
	}
	if runtime.GOOS == "darwin" {
		candidates = append(candidates, "/opt/homebrew/bin/"+name)
	}

	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

// execCLI runs a CLI command and returns any error.
func execCLI(path string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
