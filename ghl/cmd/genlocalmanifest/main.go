package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"gopkg.in/yaml.v3"
)

func main() {
	repoRoot := mustFindRepoRoot()
	defaultWorkspace := filepath.Dir(repoRoot)

	workspaceRoot := flag.String("workspace-root", defaultWorkspace, "Workspace root containing local Git repos")
	inputPath := flag.String("input", filepath.Join(repoRoot, "REPOS.yaml"), "Source manifest path")
	outputPath := flag.String("output", filepath.Join(repoRoot, "REPOS.local.yaml"), "Generated local manifest path")
	flag.Parse()

	m, err := manifest.Load(*inputPath)
	if err != nil {
		exitf("load manifest: %v", err)
	}

	localRemotes, localDirs, err := scanWorkspace(*workspaceRoot)
	if err != nil {
		exitf("scan workspace: %v", err)
	}

	filtered := manifest.Manifest{Repos: make([]manifest.Repo, 0, len(m.Repos))}
	for _, repo := range m.Repos {
		if localRemotes[canonicalGitHubURL(repo.GitHubURL)] || localDirs[repo.Name] {
			filtered.Repos = append(filtered.Repos, repo)
		}
	}

	if err := writeManifest(*outputPath, *workspaceRoot, *inputPath, filtered); err != nil {
		exitf("write manifest: %v", err)
	}

	fmt.Printf("generated %s with %d repos (from %d total)\n", *outputPath, len(filtered.Repos), len(m.Repos))
}

func mustFindRepoRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		exitf("getwd: %v", err)
	}
	current := wd
	for {
		if _, err := os.Stat(filepath.Join(current, "REPOS.yaml")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			exitf("could not locate repo root from %s", wd)
		}
		current = parent
	}
}

func scanWorkspace(workspaceRoot string) (map[string]bool, map[string]bool, error) {
	entries, err := os.ReadDir(workspaceRoot)
	if err != nil {
		return nil, nil, err
	}

	remotes := make(map[string]bool, len(entries))
	dirs := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		repoDir := filepath.Join(workspaceRoot, entry.Name())
		if _, err := os.Stat(filepath.Join(repoDir, ".git")); err != nil {
			continue
		}
		dirs[entry.Name()] = true
		remote, err := gitRemote(repoDir)
		if err != nil {
			continue
		}
		remotes[canonicalGitHubURL(remote)] = true
	}
	return remotes, dirs, nil
}

func gitRemote(repoDir string) (string, error) {
	cmd := exec.Command("git", "-C", repoDir, "remote", "get-url", "origin")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func canonicalGitHubURL(raw string) string {
	url := strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(url, "git@github.com:"):
		url = "https://github.com/" + strings.TrimPrefix(url, "git@github.com:")
	case strings.HasPrefix(url, "ssh://git@github.com/"):
		url = "https://github.com/" + strings.TrimPrefix(url, "ssh://git@github.com/")
	}
	url = strings.TrimSuffix(url, ".git")
	url = strings.TrimRight(url, "/")
	return strings.ToLower(url)
}

func writeManifest(outputPath, workspaceRoot, inputPath string, m manifest.Manifest) error {
	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	header := []string{
		"# REPOS.local.yaml — generated local fleet manifest",
		fmt.Sprintf("# workspace_root: %s", workspaceRoot),
		fmt.Sprintf("# source_manifest: %s", inputPath),
		"# Regenerate from ./ghl with: go run ./cmd/genlocalmanifest",
		"",
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0750); err != nil {
		return err
	}
	return os.WriteFile(outputPath, []byte(strings.Join(header, "\n")+string(data)), 0644)
}

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
