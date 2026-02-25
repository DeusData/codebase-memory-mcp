package pipeline

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
)

// EnvBinding represents an extracted environment variable with a URL value.
type EnvBinding struct {
	Key      string
	Value    string
	FilePath string // relative path where found
}

// ScanProjectEnvURLs walks the project root, scanning all non-ignored files for
// env var assignments where the value looks like a URL.
func ScanProjectEnvURLs(rootPath string) []EnvBinding {
	var bindings []EnvBinding

	_ = filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		name := info.Name()

		// Skip ignored directories (reuse discover package patterns)
		if info.IsDir() {
			if discover.IGNORE_PATTERNS[name] {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip secret files
		if isSecretFile(name) {
			return nil
		}

		// Skip ignored suffixes
		for suffix := range discover.IGNORE_SUFFIXES {
			if strings.HasSuffix(name, suffix) {
				return nil
			}
		}

		// Skip binary/large files
		if info.Size() > 1<<20 { // 1MB
			return nil
		}

		relPath, _ := filepath.Rel(rootPath, path)
		if relPath == "" {
			return nil
		}

		fileBindings := scanFileForEnvURLs(path, relPath)
		bindings = append(bindings, fileBindings...)

		return nil
	})

	return bindings
}

func scanFileForEnvURLs(absPath, relPath string) []EnvBinding {
	name := filepath.Base(absPath)
	ext := filepath.Ext(name)
	lowerName := strings.ToLower(name)

	// Determine which patterns to use based on file type
	var patterns []*regexp.Regexp

	switch {
	case isDockerfile(lowerName):
		patterns = dockerfilePatterns
	case ext == ".yaml" || ext == ".yml":
		patterns = yamlPatterns
	case ext == ".tf" || ext == ".hcl":
		patterns = terraformPatterns
	case ext == ".sh" || ext == ".bash" || ext == ".zsh":
		patterns = shellPatterns
	case isEnvFile(lowerName):
		patterns = envFilePatterns
	case ext == ".toml":
		patterns = tomlPatterns
	case ext == ".properties" || ext == ".cfg" || ext == ".ini":
		patterns = propertiesPatterns
	default:
		// Not a config file type we scan
		return nil
	}

	return scanWithPatterns(absPath, relPath, patterns)
}

// Pattern definitions (compiled once as package-level vars).
var (
	// Dockerfile: ENV KEY=VALUE or ENV KEY VALUE, ARG KEY=VALUE
	dockerfilePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^(?:ENV|ARG)\s+(\w+)[= ](.*)`),
	}

	// YAML: key: "https://..." or --set-env-vars KEY=VALUE
	yamlPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(\w+):\s*["']?(https?://[^\s"']+)`),
		regexp.MustCompile(`--set-env-vars\s+(\w+)=(\S+)`),
	}

	// Terraform/HCL: default = "https://..." or value = "https://..."
	terraformPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?:default|value)\s*=\s*"(https?://[^"]+)"`),
	}

	// Shell: export KEY="https://..." or KEY="https://..."
	shellPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?:export\s+)?(\w+)=["']?(https?://[^\s"']+)`),
	}

	// .env files: KEY=https://...
	envFilePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^(\w+)=(https?://\S+)`),
	}

	// TOML: key = "https://..."
	tomlPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(\w+)\s*=\s*"(https?://[^"]+)"`),
	}

	// Properties/cfg/ini: key=https://...
	propertiesPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(\w+)\s*=\s*(https?://\S+)`),
	}
)

func isDockerfile(name string) bool {
	lower := strings.ToLower(name)
	if lower == "dockerfile" {
		return true
	}
	if strings.HasPrefix(lower, "dockerfile.") || strings.HasSuffix(lower, ".dockerfile") {
		return true
	}
	return false
}

func isEnvFile(name string) bool {
	lower := strings.ToLower(name)
	if lower == ".env" || strings.HasPrefix(lower, ".env.") || strings.HasSuffix(lower, ".env") {
		return true
	}
	return false
}

// Secret exclusion patterns.
var secretKeyPattern = regexp.MustCompile(
	`(?i)(secret|password|passwd|token|api_key|apikey|private_key|` +
		`credential|auth_token|access_key|client_secret|signing_key|` +
		`encryption_key|ssh_key|deploy_key|service_account|bearer|jwt_secret)`)

var secretValuePattern = regexp.MustCompile(
	`(?i)(-----BEGIN|AKIA[0-9A-Z]{16}|sk-[a-zA-Z0-9]{20,}|` +
		`ghp_[a-zA-Z0-9]{36}|glpat-[a-zA-Z0-9\-]{20,}|xox[bps]-[a-zA-Z0-9\-]+)`)

func isSecretFile(name string) bool {
	lower := strings.ToLower(name)
	secretFilePatterns := []string{
		"service_account", "credentials", "key.json", "key.pem",
		"id_rsa", "id_ed25519", ".pem", ".key",
	}
	for _, p := range secretFilePatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

func isSecretBinding(key, value string) bool {
	if secretKeyPattern.MatchString(key) {
		return true
	}
	if secretValuePattern.MatchString(value) {
		return true
	}
	return false
}

// isSecretValue checks only the value pattern, not the key name.
// Use for compose/infra files where key names like JWT_PRIVATE_KEY_ID
// are config references, not actual secrets.
func isSecretValue(value string) bool {
	return secretValuePattern.MatchString(value)
}

func scanWithPatterns(absPath, relPath string, patterns []*regexp.Regexp) []EnvBinding {
	f, err := os.Open(absPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	var bindings []EnvBinding
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip comments
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}

		for _, pat := range patterns {
			matches := pat.FindStringSubmatch(line)
			if matches == nil {
				continue
			}

			var key, value string
			switch len(matches) {
			case 2:
				// Pattern with only value capture (terraform: default = "URL")
				key = "_tf_default"
				value = matches[1]
			case 3:
				key = matches[1]
				value = strings.Trim(matches[2], `"'`)
			default:
				continue
			}

			// Validate: must be a URL
			if !strings.HasPrefix(value, "http://") && !strings.HasPrefix(value, "https://") {
				continue
			}

			// Secret exclusion
			if isSecretBinding(key, value) {
				continue
			}

			bindings = append(bindings, EnvBinding{
				Key:      key,
				Value:    value,
				FilePath: relPath,
			})
		}
	}

	return bindings
}
