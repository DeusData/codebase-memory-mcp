package pipeline

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// infraFile holds parsed metadata from an infrastructure file.
type infraFile struct {
	relPath    string
	infraType  string
	properties map[string]any
}

// passInfraFiles walks the repo, detects infrastructure files, parses them,
// and creates File + InfraFile nodes for each.
func (p *Pipeline) passInfraFiles() {
	var infras []infraFile

	_ = filepath.Walk(p.RepoPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if discover.IGNORE_PATTERNS[info.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		for suffix := range discover.IGNORE_SUFFIXES {
			if strings.HasSuffix(info.Name(), suffix) {
				return nil
			}
		}
		if info.Size() > 1<<20 {
			return nil
		}

		relPath, _ := filepath.Rel(p.RepoPath, path)
		if relPath == "" {
			return nil
		}
		relPath = filepath.ToSlash(relPath)

		parsed := parseInfraFile(path, relPath, info.Name())
		infras = append(infras, parsed...)
		return nil
	})

	if len(infras) == 0 {
		return
	}

	for _, inf := range infras {
		p.upsertInfraNodes(inf)
	}
	slog.Info("pass4.infra", "nodes", len(infras))
}

// parseInfraFile routes a file to the appropriate parser based on its name.
func parseInfraFile(absPath, relPath, name string) []infraFile {
	lower := strings.ToLower(name)

	ext := strings.ToLower(filepath.Ext(name))

	switch {
	case isDockerfile(lower):
		return parseDockerfile(absPath, relPath)
	case isComposeFile(lower):
		return parseComposeFile(absPath, relPath)
	case isCloudbuildFile(lower):
		return parseCloudbuildFile(absPath, relPath)
	case isEnvFile(lower):
		return parseDotenvFile(absPath, relPath)
	case ext == ".tf":
		return parseTerraformFile(absPath, relPath)
	case isShellScript(lower, ext):
		return parseShellScript(absPath, relPath)
	default:
		return nil
	}
}

// isComposeFile returns true for docker-compose/compose YAML files.
func isComposeFile(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "docker-compose") || lower == "compose.yml" || lower == "compose.yaml" {
		ext := filepath.Ext(lower)
		return ext == ".yml" || ext == ".yaml"
	}
	return false
}

// isCloudbuildFile returns true for cloudbuild YAML files.
func isCloudbuildFile(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "cloudbuild") {
		ext := filepath.Ext(lower)
		return ext == ".yml" || ext == ".yaml"
	}
	return false
}

// upsertInfraNodes creates a File node (for search_code) and an InfraFile node
// (for structured graph queries) for each infrastructure file.
func (p *Pipeline) upsertInfraNodes(inf infraFile) {
	// File node — enables search_code (which queries label="File")
	fileQN := fqn.Compute(p.ProjectName, inf.relPath, "") + ".__file__"
	_, _ = p.Store.UpsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         "File",
		Name:          filepath.Base(inf.relPath),
		QualifiedName: fileQN,
		FilePath:      inf.relPath,
		Properties:    map[string]any{"extension": filepath.Ext(inf.relPath)},
	})

	// InfraFile node — structured metadata queryable via query_graph
	infraQN := p.infraQN(inf.relPath, inf.properties)
	_, _ = p.Store.UpsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         "InfraFile",
		Name:          filepath.Base(inf.relPath),
		QualifiedName: infraQN,
		FilePath:      inf.relPath,
		Properties:    inf.properties,
	})
}

// infraQN builds the qualified name for an InfraFile node.
// For compose services: project.path.docker-compose::service-name
// For others: project.path.__infra__
func (p *Pipeline) infraQN(relPath string, props map[string]any) string {
	base := fqn.Compute(p.ProjectName, relPath, "")

	// Compose services get a per-service QN suffix
	if sn, ok := props["service_name"].(string); ok && props["infra_type"] == "compose-service" {
		return base + "::" + sn
	}
	return base + ".__infra__"
}

// --- Dockerfile parser (regex) ---

var (
	reFrom        = regexp.MustCompile(`(?i)^FROM\s+(\S+)(?:\s+AS\s+(\w+))?`)
	reExpose      = regexp.MustCompile(`(?i)^EXPOSE\s+(.+)`)
	reEnv         = regexp.MustCompile(`(?i)^ENV\s+(\w+)[= ](.+)`)
	reArg         = regexp.MustCompile(`(?i)^ARG\s+(\w+)(?:=(.+))?`)
	reWorkdir     = regexp.MustCompile(`(?i)^WORKDIR\s+(.+)`)
	reCmd         = regexp.MustCompile(`(?i)^CMD\s+(.+)`)
	reEntrypoint  = regexp.MustCompile(`(?i)^ENTRYPOINT\s+(.+)`)
	reUser        = regexp.MustCompile(`(?i)^USER\s+(\w+)`)
	reHealthcheck = regexp.MustCompile(`(?i)^HEALTHCHECK\s+.*CMD\s+(.+)`)
)

func parseDockerfile(absPath, relPath string) []infraFile {
	f, err := os.Open(absPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	var d dockerfileState
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		d.parseLine(line)
	}

	props := d.toProperties()
	if len(props) == 0 {
		return nil
	}
	return []infraFile{{relPath: relPath, infraType: "dockerfile", properties: props}}
}

// dockerfileState accumulates parsed Dockerfile directives.
type dockerfileState struct {
	stages  []map[string]string
	ports   []string
	envVars map[string]string
	args    []string
	workdir string
	cmd     string
	entry   string
	health  string
	user    string
}

func (d *dockerfileState) parseLine(line string) {
	d.parseFrom(line)
	d.parseExpose(line)
	d.parseEnvArg(line)
	d.parseDirectives(line)
}

func (d *dockerfileState) parseFrom(line string) {
	if m := reFrom.FindStringSubmatch(line); m != nil {
		stage := map[string]string{"image": m[1]}
		if m[2] != "" {
			stage["name"] = m[2]
		}
		d.stages = append(d.stages, stage)
	}
}

func (d *dockerfileState) parseExpose(line string) {
	if m := reExpose.FindStringSubmatch(line); m != nil {
		ports := strings.Fields(m[1])
		for _, port := range ports {
			// Strip protocol suffix (e.g. 8080/tcp)
			port = strings.Split(port, "/")[0]
			d.ports = append(d.ports, port)
		}
	}
}

func (d *dockerfileState) parseEnvArg(line string) {
	if m := reEnv.FindStringSubmatch(line); m != nil {
		key, val := m[1], strings.TrimSpace(m[2])
		if !isSecretBinding(key, val) {
			if d.envVars == nil {
				d.envVars = make(map[string]string)
			}
			d.envVars[key] = val
		}
	}
	if m := reArg.FindStringSubmatch(line); m != nil {
		d.args = append(d.args, m[1])
	}
}

func (d *dockerfileState) parseDirectives(line string) {
	if m := reWorkdir.FindStringSubmatch(line); m != nil {
		d.workdir = strings.TrimSpace(m[1])
	}
	if m := reCmd.FindStringSubmatch(line); m != nil {
		d.cmd = cleanJSONBrackets(strings.TrimSpace(m[1]))
	}
	if m := reEntrypoint.FindStringSubmatch(line); m != nil {
		d.entry = cleanJSONBrackets(strings.TrimSpace(m[1]))
	}
	if m := reUser.FindStringSubmatch(line); m != nil {
		d.user = m[1]
	}
	if m := reHealthcheck.FindStringSubmatch(line); m != nil {
		d.health = strings.TrimSpace(m[1])
	}
}

func (d *dockerfileState) toProperties() map[string]any {
	if len(d.stages) == 0 {
		return nil
	}

	props := map[string]any{
		"infra_type": "dockerfile",
	}

	// base_image = last stage's image (final runtime image)
	lastImage := d.stages[len(d.stages)-1]["image"]
	props["base_image"] = lastImage

	// base_images = all unique images
	images := make([]string, 0, len(d.stages))
	for _, s := range d.stages {
		images = append(images, s["image"])
	}
	props["base_images"] = images

	// stages with name/image
	props["stages"] = d.stages

	setNonEmpty(props, "exposed_ports", d.ports)
	setNonEmptyMap(props, "env_vars", d.envVars)
	setNonEmpty(props, "build_args", d.args)
	setNonEmptyStr(props, "workdir", d.workdir)
	setNonEmptyStr(props, "cmd", d.cmd)
	setNonEmptyStr(props, "entrypoint", d.entry)
	setNonEmptyStr(props, "healthcheck", d.health)
	setNonEmptyStr(props, "user", d.user)

	return props
}

// --- .env file parser ---

var reDotenvLine = regexp.MustCompile(`^([A-Za-z_]\w*)=(.*)$`)

func parseDotenvFile(absPath, relPath string) []infraFile {
	f, err := os.Open(absPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	envVars := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if m := reDotenvLine.FindStringSubmatch(line); m != nil {
			key, val := m[1], strings.Trim(m[2], `"'`)
			if !isSecretBinding(key, val) {
				envVars[key] = val
			}
		}
	}

	if len(envVars) == 0 {
		return nil
	}

	props := map[string]any{
		"infra_type": "env",
		"env_vars":   envVars,
	}
	return []infraFile{{relPath: relPath, infraType: "env", properties: props}}
}

// --- Shell script parser ---

// isShellScript returns true for shell script files.
func isShellScript(name, ext string) bool {
	_ = name // reserved for future name-based detection
	return ext == ".sh" || ext == ".bash" || ext == ".zsh"
}

var (
	reShebang    = regexp.MustCompile(`^#!\s*(.+)`)
	reExportVar  = regexp.MustCompile(`^export\s+(\w+)=(.*)`)
	reShellVar   = regexp.MustCompile(`^(\w+)=["']?([^"'\n]*)["']?`)
	reSourceFile = regexp.MustCompile(`^(?:source|\.) ["']?(\S+)["']?`)
	reDockerCmd  = regexp.MustCompile(`^(docker(?:-compose)?)\s+(\w+)`)
)

func parseShellScript(absPath, relPath string) []infraFile {
	f, err := os.Open(absPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	var st shellState
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		st.parseLine(line)
	}

	props := st.toProperties()
	if len(props) <= 1 {
		return nil
	}
	return []infraFile{{relPath: relPath, infraType: "shell", properties: props}}
}

// shellState accumulates parsed shell script metadata.
type shellState struct {
	shebang    string
	envVars    map[string]string
	sources    []string
	dockerCmds []string
}

func (s *shellState) parseLine(line string) {
	if line == "" {
		return
	}

	// Shebang (first non-empty line starting with #!)
	if s.shebang == "" {
		if m := reShebang.FindStringSubmatch(line); m != nil {
			s.shebang = strings.TrimSpace(m[1])
			return
		}
	}

	// Skip comments
	if strings.HasPrefix(line, "#") {
		return
	}

	s.parseVarsAndCmds(line)
}

func (s *shellState) parseVarsAndCmds(line string) {
	// Exported env vars
	if m := reExportVar.FindStringSubmatch(line); m != nil {
		s.addEnvVar(m[1], strings.Trim(m[2], `"'`))
		return
	}

	// Plain var assignments (only at line start, not inside commands)
	if m := reShellVar.FindStringSubmatch(line); m != nil && !strings.Contains(line, " ") {
		s.addEnvVar(m[1], m[2])
		return
	}

	// Source/dot includes
	if m := reSourceFile.FindStringSubmatch(line); m != nil {
		s.sources = append(s.sources, m[1])
		return
	}

	// Docker commands
	if m := reDockerCmd.FindStringSubmatch(line); m != nil {
		s.dockerCmds = append(s.dockerCmds, m[1]+" "+m[2])
	}
}

func (s *shellState) addEnvVar(key, val string) {
	if isSecretBinding(key, val) {
		return
	}
	if s.envVars == nil {
		s.envVars = make(map[string]string)
	}
	s.envVars[key] = val
}

func (s *shellState) toProperties() map[string]any {
	props := map[string]any{
		"infra_type": "shell",
	}
	setNonEmptyStr(props, "shebang", s.shebang)
	setNonEmptyMap(props, "env_vars", s.envVars)
	setNonEmpty(props, "sources", s.sources)
	setNonEmpty(props, "docker_commands", s.dockerCmds)
	return props
}

// --- Property helpers ---

func setNonEmpty(m map[string]any, key string, val []string) {
	if len(val) > 0 {
		m[key] = val
	}
}

func setNonEmptyMap(m map[string]any, key string, val map[string]string) {
	if len(val) > 0 {
		m[key] = val
	}
}

func setNonEmptyStr(m map[string]any, key, val string) {
	if val != "" {
		m[key] = val
	}
}

// cleanJSONBrackets strips JSON array brackets from CMD/ENTRYPOINT values.
// e.g. ["./app", "--flag"] → ./app --flag
func cleanJSONBrackets(s string) string {
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		inner := s[1 : len(s)-1]
		inner = strings.ReplaceAll(inner, `"`, "")
		inner = strings.ReplaceAll(inner, ",", " ")
		return collapseSpaces(strings.TrimSpace(inner))
	}
	return s
}

// collapseSpaces replaces runs of whitespace with a single space.
func collapseSpaces(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if !inSpace {
				b.WriteByte(' ')
				inSpace = true
			}
		} else {
			b.WriteRune(r)
			inSpace = false
		}
	}
	return b.String()
}
