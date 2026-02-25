package pipeline

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// --- docker-compose parser ---

// composeFile represents the top-level structure of a docker-compose file.
type composeFile struct {
	Services map[string]composeService `yaml:"services"`
}

// composeService represents a single service in docker-compose.
type composeService struct {
	Build         any               `yaml:"build"`
	Image         string            `yaml:"image"`
	Ports         []string          `yaml:"ports"`
	Expose        []string          `yaml:"expose"`
	Environment   any               `yaml:"environment"`
	DependsOn     any               `yaml:"depends_on"`
	Networks      any               `yaml:"networks"`
	ContainerName string            `yaml:"container_name"`
	Volumes       []string          `yaml:"volumes"`
	Command       any               `yaml:"command"`
	Labels        map[string]string `yaml:"labels"`
}

func parseComposeFile(absPath, relPath string) []infraFile {
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil
	}

	var cf composeFile
	if err := yaml.Unmarshal(data, &cf); err != nil {
		return nil
	}

	if len(cf.Services) == 0 {
		return nil
	}

	result := make([]infraFile, 0, len(cf.Services))
	for name := range cf.Services {
		svc := cf.Services[name]
		props := buildComposeServiceProps(name, &svc)
		result = append(result, infraFile{
			relPath:    relPath,
			infraType:  "compose-service",
			properties: props,
		})
	}
	return result
}

func buildComposeServiceProps(name string, svc *composeService) map[string]any {
	props := map[string]any{
		"infra_type":   "compose-service",
		"service_name": name,
	}

	setNonEmptyStr(props, "image", svc.Image)
	setNonEmptyStr(props, "container_name", svc.ContainerName)
	setBuildContext(props, svc.Build)
	setNonEmpty(props, "ports", svc.Ports)
	setNonEmpty(props, "expose", svc.Expose)
	setNonEmpty(props, "volumes", svc.Volumes)

	envMap := parseComposeEnvironment(svc.Environment)
	setNonEmptyMap(props, "environment", envMap)

	deps := parseDependsOn(svc.DependsOn)
	setNonEmpty(props, "depends_on", deps)

	nets := parseNetworks(svc.Networks)
	setNonEmpty(props, "networks", nets)

	return props
}

// setBuildContext extracts the build context from various docker-compose formats.
// build can be a string or a map with "context" key.
func setBuildContext(props map[string]any, build any) {
	switch v := build.(type) {
	case string:
		setNonEmptyStr(props, "build_context", v)
	case map[string]any:
		if ctx, ok := v["context"].(string); ok {
			setNonEmptyStr(props, "build_context", ctx)
		}
	}
}

// parseComposeEnvironment handles both map and list formats for environment.
func parseComposeEnvironment(env any) map[string]string {
	if env == nil {
		return nil
	}
	result := make(map[string]string)

	switch v := env.(type) {
	case map[string]any:
		for k, val := range v {
			if s, ok := val.(string); ok && !isSecretBinding(k, s) {
				result[k] = s
			}
		}
	case []any:
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				continue
			}
			parts := strings.SplitN(s, "=", 2)
			if len(parts) == 2 && !isSecretBinding(parts[0], parts[1]) {
				result[parts[0]] = parts[1]
			}
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

// parseDependsOn handles both list and map formats for depends_on.
func parseDependsOn(dep any) []string {
	if dep == nil {
		return nil
	}
	switch v := dep.(type) {
	case []any:
		var result []string
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case map[string]any:
		var result []string
		for name := range v {
			result = append(result, name)
		}
		return result
	}
	return nil
}

// parseNetworks handles both list and map formats for networks.
func parseNetworks(nets any) []string {
	if nets == nil {
		return nil
	}
	switch v := nets.(type) {
	case []any:
		var result []string
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case map[string]any:
		var result []string
		for name := range v {
			result = append(result, name)
		}
		return result
	}
	return nil
}

// --- cloudbuild parser ---

// cloudbuildFile represents the top-level structure of a Cloud Build YAML.
type cloudbuildFile struct {
	Steps         []cloudbuildStep  `yaml:"steps"`
	Substitutions map[string]string `yaml:"substitutions"`
	Images        []string          `yaml:"images"`
	Options       map[string]any    `yaml:"options"`
}

type cloudbuildStep struct {
	Name string   `yaml:"name"`
	Args []string `yaml:"args"`
	Env  []string `yaml:"env"`
}

func parseCloudbuildFile(absPath, relPath string) []infraFile {
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil
	}

	var cb cloudbuildFile
	if err := yaml.Unmarshal(data, &cb); err != nil {
		return nil
	}

	props := buildCloudbuildProps(cb)
	props["infra_type"] = "cloudbuild"

	return []infraFile{{relPath: relPath, infraType: "cloudbuild", properties: props}}
}

func buildCloudbuildProps(cb cloudbuildFile) map[string]any {
	props := make(map[string]any)

	if len(cb.Substitutions) > 0 {
		filtered := filterSecretMap(cb.Substitutions)
		setNonEmptyMap(props, "substitutions", filtered)
		if sn, ok := cb.Substitutions["_SERVICE_NAME"]; ok {
			props["service_name"] = sn
		}
	}

	if len(cb.Images) > 0 {
		props["image_registry"] = cb.Images[0]
	}

	// Walk steps looking for gcloud run deploy
	extractDeployFlags(cb.Steps, props)

	return props
}

// extractDeployFlags scans cloudbuild steps for gcloud run deploy commands
// and extracts deployment flags.
func extractDeployFlags(steps []cloudbuildStep, props map[string]any) {
	for _, step := range steps {
		if !isDeployStep(step.Args) {
			continue
		}
		parseDeployArgs(step.Args, props)
		parseDeployEnvVars(step.Args, props)
		return // only process first deploy step
	}
}

// isDeployStep returns true if the args list contains "gcloud run deploy" sequence.
func isDeployStep(args []string) bool {
	if len(args) < 3 {
		return false
	}
	// Match "run" + "deploy" anywhere in args (gcloud run deploy ...)
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "run" && args[i+1] == "deploy" {
			return true
		}
	}
	return false
}

// deployFlagMap maps gcloud run deploy flags to property names.
var deployFlagMap = map[string]string{
	"--region":          "deploy_region",
	"--cpu":             "deploy_cpu",
	"--memory":          "deploy_memory",
	"--concurrency":     "deploy_concurrency",
	"--max-instances":   "deploy_max_instances",
	"--timeout":         "deploy_timeout",
	"--ingress":         "deploy_ingress",
	"--image":           "image_registry",
	"--service-account": "deploy_service_account",
}

func parseDeployArgs(args []string, props map[string]any) {
	for i, arg := range args {
		// --flag=value format
		if idx := strings.Index(arg, "="); idx > 0 {
			flag := arg[:idx]
			val := arg[idx+1:]
			if propName, ok := deployFlagMap[flag]; ok {
				props[propName] = val
			}
			continue
		}
		// --flag value format (next arg is value)
		if propName, ok := deployFlagMap[arg]; ok && i+1 < len(args) {
			props[propName] = args[i+1]
		}
	}
}

// parseDeployEnvVars extracts --set-env-vars or --update-env-vars from deploy args.
func parseDeployEnvVars(args []string, props map[string]any) {
	for i, arg := range args {
		envStr := extractEnvVarsFlag(arg, args, i)
		if envStr == "" {
			continue
		}

		envMap := make(map[string]string)
		for _, pair := range strings.Split(envStr, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 && !isSecretBinding(parts[0], parts[1]) {
				envMap[parts[0]] = parts[1]
			}
		}
		setNonEmptyMap(props, "deploy_env_vars", envMap)
		return
	}
}

// extractEnvVarsFlag extracts the env vars string from --set-env-vars or --update-env-vars flags.
func extractEnvVarsFlag(arg string, args []string, i int) string {
	switch {
	case strings.HasPrefix(arg, "--set-env-vars="):
		return arg[len("--set-env-vars="):]
	case strings.HasPrefix(arg, "--update-env-vars="):
		return arg[len("--update-env-vars="):]
	case (arg == "--set-env-vars" || arg == "--update-env-vars") && i+1 < len(args):
		return args[i+1]
	default:
		return ""
	}
}

// filterSecretMap returns a copy of the map with secret key/values removed.
func filterSecretMap(m map[string]string) map[string]string {
	filtered := make(map[string]string, len(m))
	for k, v := range m {
		if !isSecretBinding(k, v) {
			filtered[k] = v
		}
	}
	return filtered
}
