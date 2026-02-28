package pipeline

import (
	"log/slog"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passInherits creates INHERITS edges from Class nodes to their base classes.
// Reads base_classes property (set during extractClassDef) and resolves via registry.
func (p *Pipeline) passInherits() {
	slog.Info("pass.inherits")

	count := 0
	for _, label := range []string{"Class", "Type", "Interface", "Enum"} {
		nodes, err := p.Store.FindNodesByLabel(p.ProjectName, label)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			bases, ok := n.Properties["base_classes"]
			if !ok {
				continue
			}
			baseList, ok := bases.([]any)
			if !ok {
				continue
			}

			moduleQN := qualifiedNamePrefix(n.QualifiedName)
			importMap := p.importMaps[moduleQN]

			for _, b := range baseList {
				baseName, ok := b.(string)
				if !ok || baseName == "" {
					continue
				}

				// Resolve base class to a registered Class/Type/Interface
				targetQN := resolveAsClass(baseName, p.registry, moduleQN, importMap)
				if targetQN == "" {
					continue
				}

				targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, targetQN)
				if targetNode == nil {
					continue
				}

				_, _ = p.Store.InsertEdge(&store.Edge{
					Project:  p.ProjectName,
					SourceID: n.ID,
					TargetID: targetNode.ID,
					Type:     "INHERITS",
				})
				count++
			}
		}
	}

	slog.Info("pass.inherits.done", "edges", count)
}

// qualifiedNamePrefix returns the module QN portion of a fully qualified name.
// e.g., "project.path.module.ClassName" → "project.path.module"
func qualifiedNamePrefix(qn string) string {
	for i := len(qn) - 1; i >= 0; i-- {
		if qn[i] == '.' {
			return qn[:i]
		}
	}
	return qn
}
