// Package manifest loads and validates the GHL fleet repos manifest (REPOS.yaml).
package manifest

import (
	"fmt"
	"io"
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

// Repo describes a single GHL GitHub repository to be indexed.
type Repo struct {
	Name      string   `yaml:"name"`
	GitHubURL string   `yaml:"github_url"`
	Team      string   `yaml:"team"`
	Type      string   `yaml:"type"` // "backend" | "frontend" | "infra" | "other"
	Tags      []string `yaml:"tags"`
}

// Validate returns an error if the repo is missing required fields or has invalid values.
func (r Repo) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("repo: name is required")
	}
	if r.GitHubURL == "" {
		return fmt.Errorf("repo %q: github_url is required", r.Name)
	}
	u, err := url.ParseRequestURI(r.GitHubURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("repo %q: invalid github_url %q", r.Name, r.GitHubURL)
	}
	return nil
}

// Slug returns the last path component of GitHubURL (the repo name on disk).
func (r Repo) Slug() string {
	return r.Name
}

// Manifest is the parsed top-level structure of REPOS.yaml.
type Manifest struct {
	Repos []Repo `yaml:"repos"`
}

// FindByName returns the repo with the given name, or false if not found.
func (m *Manifest) FindByName(name string) (Repo, bool) {
	for _, r := range m.Repos {
		if r.Name == name {
			return r, true
		}
	}
	return Repo{}, false
}

// FilterByTeam returns all repos belonging to the given team.
func (m *Manifest) FilterByTeam(team string) []Repo {
	var out []Repo
	for _, r := range m.Repos {
		if r.Team == team {
			out = append(out, r)
		}
	}
	return out
}

// Load reads and validates the manifest from a file path.
func Load(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: open %q: %w", path, err)
	}
	defer f.Close()
	return LoadReader(f)
}

// LoadReader reads and validates the manifest from an io.Reader.
func LoadReader(r io.Reader) (*Manifest, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("manifest: read: %w", err)
	}

	var m Manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest: parse YAML: %w", err)
	}

	for i, repo := range m.Repos {
		if err := repo.Validate(); err != nil {
			return nil, fmt.Errorf("manifest: repo[%d]: %w", i, err)
		}
	}

	return &m, nil
}
