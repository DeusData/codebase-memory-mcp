package pipeline

import (
	"log/slog"
	"runtime"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
)

// passConfigures creates CONFIGURES edges from functions that read env vars
// to config Module nodes that define them.
func (p *Pipeline) passConfigures() {
	slog.Info("pass.configures")

	// Stage 1: Build env key → module QN index from Module constants
	envIndex := p.buildEnvIndex()
	if len(envIndex) == 0 {
		slog.Info("pass.configures.skip", "reason", "no_env_bindings")
		return
	}

	// Stage 2: Parallel per-file AST walk for env access calls
	type fileEntry struct {
		relPath string
		cached  *cachedAST
	}
	files := make([]fileEntry, 0, len(p.astCache))
	for relPath, cached := range p.astCache {
		spec := lang.ForLanguage(cached.Language)
		if spec == nil {
			continue
		}
		if len(spec.EnvAccessFunctions) == 0 && len(spec.EnvAccessMemberPatterns) == 0 {
			continue
		}
		files = append(files, fileEntry{relPath, cached})
	}

	if len(files) == 0 {
		return
	}

	results := make([][]resolvedEdge, len(files))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files) {
		numWorkers = len(files)
	}

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, fe := range files {
		g.Go(func() error {
			results[i] = p.resolveFileConfigures(fe.relPath, fe.cached, envIndex)
			return nil
		})
	}
	_ = g.Wait()

	// Stage 3: Batch write
	p.flushResolvedEdges(results)

	total := 0
	for _, r := range results {
		total += len(r)
	}
	slog.Info("pass.configures.done", "edges", total)
}

// buildEnvIndex creates a mapping from env var key → module QN
// by scanning Module node constants for KEY = VALUE patterns.
func (p *Pipeline) buildEnvIndex() map[string]string {
	modules, err := p.Store.FindNodesByLabel(p.ProjectName, "Module")
	if err != nil {
		return nil
	}

	index := make(map[string]string)
	for _, m := range modules {
		constants, ok := m.Properties["constants"]
		if !ok {
			continue
		}
		constList, ok := constants.([]any)
		if !ok {
			continue
		}
		for _, c := range constList {
			constStr, ok := c.(string)
			if !ok {
				continue
			}
			parts := strings.SplitN(constStr, " = ", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				if key != "" && isEnvVarName(key) {
					index[key] = m.QualifiedName
				}
			}
		}
	}
	return index
}

func (p *Pipeline) resolveFileConfigures(relPath string, cached *cachedAST, envIndex map[string]string) []resolvedEdge {
	spec := lang.ForLanguage(cached.Language)
	if spec == nil {
		return nil
	}

	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	root := cached.Tree.RootNode()
	callTypes := toSet(spec.CallNodeTypes)

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	envFuncs := toSet(spec.EnvAccessFunctions)
	envMembers := toSet(spec.EnvAccessMemberPatterns)

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		kind := node.Kind()

		var envKey string
		switch {
		case callTypes[kind]:
			envKey = extractEnvKeyFromCall(node, cached.Source, envFuncs)
		case kind == "member_expression" || kind == "subscript" || kind == "attribute":
			envKey = extractEnvKeyFromMember(node, cached.Source, envMembers)
		default:
			return true
		}

		if envKey == "" {
			return false
		}

		targetModuleQN, ok := envIndex[envKey]
		if !ok {
			return false
		}

		funcQN := findEnclosingFunction(node, cached.Source, p.ProjectName, relPath, spec)
		if funcQN == "" {
			funcQN = moduleQN
		}

		key := [2]string{funcQN, envKey}
		if !seen[key] {
			seen[key] = true
			edges = append(edges, resolvedEdge{
				CallerQN: funcQN,
				TargetQN: targetModuleQN,
				Type:     "CONFIGURES",
				Properties: map[string]any{
					"env_key":   envKey,
					"direction": "reads",
				},
			})
		}
		return false
	})

	return edges
}

// extractEnvKeyFromCall extracts env var key from function calls like os.Getenv("KEY").
func extractEnvKeyFromCall(node *tree_sitter.Node, source []byte, envFuncs map[string]bool) string {
	funcNode := node.ChildByFieldName("function")
	if funcNode == nil {
		return ""
	}
	calleeName := parser.NodeText(funcNode, source)
	if !envFuncs[calleeName] {
		return ""
	}

	argsNode := node.ChildByFieldName("arguments")
	if argsNode == nil || argsNode.NamedChildCount() == 0 {
		return ""
	}
	firstArg := argsNode.NamedChild(0)
	if firstArg == nil {
		return ""
	}
	return unquote(parser.NodeText(firstArg, source))
}

// extractEnvKeyFromMember extracts env var key from member access like process.env.KEY.
func extractEnvKeyFromMember(node *tree_sitter.Node, source []byte, envMembers map[string]bool) string {
	text := parser.NodeText(node, source)

	for pattern := range envMembers {
		if key := matchEnvMemberPattern(text, pattern); key != "" {
			return key
		}
	}
	return ""
}

// matchEnvMemberPattern checks if text matches pattern.KEY or pattern["KEY"].
func matchEnvMemberPattern(text, pattern string) string {
	// Dot access: process.env.KEY
	prefix := pattern + "."
	if strings.HasPrefix(text, prefix) {
		key := text[len(prefix):]
		if key != "" && !strings.ContainsAny(key, ".[]()'\"") {
			return key
		}
	}

	// Subscript: os.environ["KEY"]
	bracketPrefix := pattern + "["
	if strings.HasPrefix(text, bracketPrefix) {
		inner := text[len(bracketPrefix):]
		inner = strings.TrimSuffix(inner, "]")
		return unquote(inner)
	}
	return ""
}

// isEnvVarName checks if a string looks like an environment variable name
// (uppercase with underscores).
func isEnvVarName(s string) bool {
	if len(s) < 2 {
		return false
	}
	hasUpper := false
	for _, c := range s {
		switch {
		case c >= 'A' && c <= 'Z':
			hasUpper = true
		case c == '_', c >= '0' && c <= '9':
			// ok
		default:
			return false
		}
	}
	return hasUpper
}

// unquote strips surrounding quotes from a string literal.
func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '`' && s[len(s)-1] == '`') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
