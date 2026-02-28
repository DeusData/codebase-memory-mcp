package pipeline

import (
	"log/slog"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passDecorates creates DECORATES edges from decorated functions/classes to
// the decorator function node (if it exists in the project).
func (p *Pipeline) passDecorates() {
	slog.Info("pass.decorates")

	count := 0
	for _, label := range []string{"Function", "Method", "Class"} {
		nodes, err := p.Store.FindNodesByLabel(p.ProjectName, label)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			count += p.processNodeDecorators(n)
		}
	}

	slog.Info("pass.decorates.done", "edges", count)
}

// processNodeDecorators creates DECORATES edges for a single node's decorators.
func (p *Pipeline) processNodeDecorators(n *store.Node) int {
	decs, ok := n.Properties["decorators"]
	if !ok {
		return 0
	}
	decList, ok := decs.([]any)
	if !ok {
		return 0
	}

	moduleQN := qualifiedNamePrefix(n.QualifiedName)
	importMap := p.importMaps[moduleQN]
	count := 0

	for _, d := range decList {
		decStr, ok := d.(string)
		if !ok || decStr == "" {
			continue
		}

		funcName := decoratorFunctionName(decStr)
		if funcName == "" {
			continue
		}

		targetQN := p.registry.Resolve(funcName, moduleQN, importMap)
		if targetQN == "" {
			continue
		}
		targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, targetQN)
		if targetNode == nil {
			continue
		}

		_, _ = p.Store.InsertEdge(&store.Edge{
			Project:    p.ProjectName,
			SourceID:   n.ID,
			TargetID:   targetNode.ID,
			Type:       "DECORATES",
			Properties: map[string]any{"decorator": decStr},
		})
		count++
	}
	return count
}

// decoratorFunctionName extracts the function name from a decorator string.
// "@app.route('/api')" → "app.route"
// "@pytest.fixture" → "pytest.fixture"
// "@Override" → "Override"
func decoratorFunctionName(dec string) string {
	dec = strings.TrimPrefix(dec, "@")
	if idx := strings.Index(dec, "("); idx > 0 {
		dec = dec[:idx]
	}
	return strings.TrimSpace(dec)
}
