package pipeline

import (
	"log/slog"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// ifaceInfo holds an interface node and its required method names.
type ifaceInfo struct {
	node    *store.Node
	methods []string
}

// passImplements detects Go interface satisfaction and creates IMPLEMENTS edges.
// A struct implements an interface if it has methods matching all interface methods.
func (p *Pipeline) passImplements() {
	slog.Info("pass5.implements")

	ifaces := p.collectGoInterfaces()
	if len(ifaces) == 0 {
		return
	}

	structMethods, structQNPrefix := p.collectStructMethods()
	linkCount := p.matchImplements(ifaces, structMethods, structQNPrefix)

	slog.Info("pass5.implements.done", "links", linkCount)
}

// collectGoInterfaces returns Go interfaces with their method names.
func (p *Pipeline) collectGoInterfaces() []ifaceInfo {
	interfaces, findErr := p.Store.FindNodesByLabel(p.ProjectName, "Interface")
	if findErr != nil || len(interfaces) == 0 {
		return nil
	}

	var ifaces []ifaceInfo
	for _, iface := range interfaces {
		if !strings.HasSuffix(iface.FilePath, ".go") {
			continue
		}

		edges, edgeErr := p.Store.FindEdgesBySourceAndType(iface.ID, "DEFINES_METHOD")
		if edgeErr != nil || len(edges) == 0 {
			continue
		}

		var methodNames []string
		for _, e := range edges {
			methodNode, _ := p.Store.FindNodeByID(e.TargetID)
			if methodNode != nil {
				methodNames = append(methodNames, methodNode.Name)
			}
		}

		if len(methodNames) > 0 {
			ifaces = append(ifaces, ifaceInfo{node: iface, methods: methodNames})
		}
	}
	return ifaces
}

// collectStructMethods builds maps of receiver type -> method names and QN prefixes
// from Go methods with receiver properties.
func (p *Pipeline) collectStructMethods() (structMethods map[string]map[string]bool, structQNPrefix map[string]string) {
	methodNodes, findErr := p.Store.FindNodesByLabel(p.ProjectName, "Method")
	if findErr != nil {
		return nil, nil
	}

	structMethods = make(map[string]map[string]bool)
	structQNPrefix = make(map[string]string)

	for _, m := range methodNodes {
		if !strings.HasSuffix(m.FilePath, ".go") {
			continue
		}
		recv, ok := m.Properties["receiver"]
		if !ok {
			continue
		}
		recvStr, ok := recv.(string)
		if !ok || recvStr == "" {
			continue
		}

		typeName := extractReceiverType(recvStr)
		if typeName == "" {
			continue
		}

		if structMethods[typeName] == nil {
			structMethods[typeName] = make(map[string]bool)
		}
		structMethods[typeName][m.Name] = true

		if _, exists := structQNPrefix[typeName]; !exists {
			if idx := strings.LastIndex(m.QualifiedName, "."); idx > 0 {
				structQNPrefix[typeName] = m.QualifiedName[:idx]
			}
		}
	}
	return
}

// matchImplements checks each struct against each interface and creates IMPLEMENTS edges.
func (p *Pipeline) matchImplements(
	ifaces []ifaceInfo,
	structMethods map[string]map[string]bool,
	structQNPrefix map[string]string,
) int {
	linkCount := 0
	for _, iface := range ifaces {
		for typeName, methodSet := range structMethods {
			if !satisfies(iface.methods, methodSet) {
				continue
			}

			structNode := p.findStructNode(typeName, structQNPrefix)
			if structNode == nil {
				continue
			}

			_, _ = p.Store.InsertEdge(&store.Edge{
				Project:  p.ProjectName,
				SourceID: structNode.ID,
				TargetID: iface.node.ID,
				Type:     "IMPLEMENTS",
			})
			linkCount++
		}
	}
	return linkCount
}

// findStructNode looks up the struct/class node for a given receiver type name.
func (p *Pipeline) findStructNode(typeName string, structQNPrefix map[string]string) *store.Node {
	if prefix, ok := structQNPrefix[typeName]; ok {
		structQN := prefix + "." + typeName
		if n, _ := p.Store.FindNodeByQN(p.ProjectName, structQN); n != nil {
			return n
		}
	}

	classes, _ := p.Store.FindNodesByLabel(p.ProjectName, "Class")
	for _, c := range classes {
		if c.Name == typeName && strings.HasSuffix(c.FilePath, ".go") {
			return c
		}
	}
	return nil
}

// extractReceiverType extracts the type name from a Go receiver string.
// "(h *Handlers)" -> "Handlers", "(s Store)" -> "Store"
func extractReceiverType(recv string) string {
	recv = strings.TrimSpace(recv)
	recv = strings.Trim(recv, "()")
	parts := strings.Fields(recv)
	if len(parts) == 0 {
		return ""
	}
	// Last field is the type, possibly with * prefix
	typeName := parts[len(parts)-1]
	typeName = strings.TrimPrefix(typeName, "*")
	return typeName
}

// satisfies checks if a set of method names includes all interface methods.
func satisfies(ifaceMethods []string, structMethodSet map[string]bool) bool {
	for _, m := range ifaceMethods {
		if !structMethodSet[m] {
			return false
		}
	}
	return true
}
